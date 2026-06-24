"""Multi-process repackage: shard the range jobs, fetch each shard in its own process
(one asyncio event loop per CPU core), then merge the shards into a single WARC.

A single asyncio loop saturates one CPU core on many small range-GETs, so reaching
all cores needs multiple processes. This orchestrator keeps that entirely inside
``cdxt repackage`` (no external driver):

1. Drain the configured source once and split it into N self-contained shard CSVs,
   hashed by WARC filename so every record of a file stays in one shard.
2. Run one worker process per shard concurrently. Each worker **rotates its output at
   ``--size``** into one or more ``<prefix>-shNN-001.warc.gz``, ``-002`` … files, every
   file a self-contained WARC with its own ``warcinfo`` record.
3. **Renumber** all shard files (in shard order, sequence order within a shard) into one
   global ``<prefix>-001.warc.gz``, ``-002`` … series via a concurrent copy (server-side
   on S3), then delete the shards. Output therefore honours ``--size`` exactly as
   single-process mode does (files are a target size; the last file of each shard may be
   short, so up to N short files appear at shard boundaries).
"""
import glob
import hashlib
import logging
import os
import shutil
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from typing import Dict, List, Optional

import boto3

from cdx_toolkit.filter_warc.sources.csv import CsvSource, RangeJobCsvWriter
from cdx_toolkit.filter_warc.warc_filter import WARCFilter
from cdx_toolkit.filter_warc.warc_utils import generate_warc_filename
from cdx_toolkit.filter_warc.s3_utils import is_s3_url, parse_s3_uri
from cdx_toolkit.filter_warc import merge as merge_mod

logger = logging.getLogger(__name__)

# Concurrency for the final renumber/copy pass (override with CDXT_MERGE_CONCURRENCY).
_RENUMBER_CONCURRENCY = int(os.environ.get('CDXT_MERGE_CONCURRENCY', '16'))


def _output_filename(prefix_path: str, sequence: int, subprefix: Optional[str] = None,
                     gzip: bool = True) -> str:
    """Build a WARC output path for a given rotation sequence (S3 uri or local path)."""
    if is_s3_url(prefix_path):
        bucket, key_prefix = parse_s3_uri(prefix_path)
        key = generate_warc_filename(key_prefix, sequence=sequence, writer_subprefix=subprefix, gzip=gzip)
        return f's3://{bucket}/{key}'
    return generate_warc_filename(prefix_path, sequence=sequence, writer_subprefix=subprefix, gzip=gzip)


def _collect_shard_files(shards_base: str, subprefix: str, aws_region_name: str) -> List[str]:
    """All files a worker actually wrote for its shard, sorted by rotation sequence.

    A worker rotates at ``--size``, so it produces an unknown number of
    ``<shards_base>-<subprefix>-NNN.warc.gz`` files; list them rather than assume a count.
    Lexical sort matches the zero-padded ``%03d`` sequence for up to 999 files.
    """
    if is_s3_url(shards_base):
        bucket, key_prefix = parse_s3_uri(shards_base)
        list_prefix = f'{key_prefix}-{subprefix}-'
        s3 = boto3.client('s3', region_name=aws_region_name)
        keys = []
        for page in s3.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=list_prefix):
            keys.extend(o['Key'] for o in page.get('Contents', []) if o['Key'].endswith('.warc.gz'))
        return [f's3://{bucket}/{k}' for k in sorted(keys)]
    return sorted(glob.glob(f'{shards_base}-{subprefix}-*.warc.gz'))


def _run_worker(cfg: Dict) -> int:
    """Worker entrypoint (runs in a child process): fetch one shard into one WARC."""
    if cfg.get('uvloop'):
        os.environ['CDXT_UVLOOP'] = '1'
    source = CsvSource(cfg['shard_csv'], warc_download_prefix=None, sort=True)
    wf = WARCFilter(
        source=source,
        prefix_path=cfg['prefix_path'],
        writer_info=cfg['writer_info'],
        writer_subprefix=cfg['subprefix'],
        write_paths_as_metadata_records=cfg['write_paths_as_metadata_records'],
        log_every_n=cfg['log_every_n'],
        # Shard CSV is self-contained (full warc_url), so CsvSource ignores this; it is
        # still needed so WARCFilter knows the read scheme (S3 vs hf://).
        warc_download_prefix=cfg['warc_download_prefix'],
        n_parallel_readers=cfg['readers'],
        aws_region_name=cfg['aws_region_name'],
        max_attempts=cfg['max_attempts'],
        max_file_size=cfg['max_file_size'],  # rotate at --size (renumbered globally later)
        # Every output file is a self-contained WARC: its own warcinfo with a fresh,
        # unique WARC-Record-ID (warcinfo_record_id=None) and a filename field naming
        # the file the writer is creating (warcinfo_filename=None).
        warcinfo_record_id=None,
        warcinfo_filename=None,
        write_warcinfo=True,
        hf_reader=cfg['hf_reader'],
    )
    return wf.filter()


def _split_source_to_shards(source, n: int, out_dir: str, record_limit: int = 0) -> List[str]:
    """Drain the source once into N self-contained shard CSVs, hashed by WARC file."""
    paths = [os.path.join(out_dir, f'shard.{i}.csv') for i in range(n)]
    writers = [RangeJobCsvWriter(p, self_contained=True) for p in paths]
    counts = [0] * n
    total = 0
    try:
        for job in source.iter_range_jobs():
            key = (job.filename or job.url or '').encode()
            idx = int(hashlib.md5(key).hexdigest(), 16) % n
            writers[idx].write(job)
            counts[idx] += 1
            total += 1
            if record_limit and total >= record_limit:
                break
    finally:
        for w in writers:
            w.close()
    logger.info('Sharded %d range jobs into %d shards: %s', total, n, counts)
    return paths


def run_multiprocess_repackage(
    *,
    source,
    n_processes: int,
    readers_per_process: int,
    prefix_path: str,
    writer_info: Dict,
    writer_subprefix: Optional[str],
    write_paths_as_metadata_records: Optional[List[str]],
    log_every_n: int,
    aws_region_name: str,
    max_attempts: int,
    record_limit: int,
    uvloop: bool,
    max_file_size: Optional[int] = 1_000_000_000,
    warc_download_prefix: Optional[str] = None,
    keep_shards: bool = False,
    hf_reader: str = 'fsspec',
) -> int:
    """Shard -> N worker processes (rotating at --size) -> renumber into one global
    <prefix>-NNN.warc.gz series. Returns the record count."""
    if writer_subprefix:
        logger.warning('--subprefix is ignored in multi-process mode (shards use shNN)')

    tmp_dir = tempfile.mkdtemp(prefix='cdxt_shards_')
    start = time.time()
    try:
        shard_csvs = _split_source_to_shards(source, n_processes, tmp_dir, record_limit)

        # The shard WARC writer supports S3 and local FS only. For an S3 final
        # destination, shards are written to S3 (renumbered server-side); otherwise
        # shards are written locally. When the final destination is a non-S3
        # fsspec backend (e.g. an hf:// bucket), stage shards in a local dir and
        # let the renumber copy stream them to the destination.
        remote_non_s3 = ('://' in prefix_path) and not is_s3_url(prefix_path)
        shards_base = os.path.join(tmp_dir, 'shard-out') if remote_non_s3 else prefix_path

        configs = []
        for i in range(n_processes):
            configs.append(dict(
                shard_csv=shard_csvs[i],
                prefix_path=shards_base,
                subprefix=f'sh{i:02d}',
                writer_info=writer_info,
                write_paths_as_metadata_records=write_paths_as_metadata_records,
                log_every_n=log_every_n,
                readers=readers_per_process,
                aws_region_name=aws_region_name,
                max_attempts=max_attempts,
                max_file_size=max_file_size,
                uvloop=uvloop,
                warc_download_prefix=warc_download_prefix,
                hf_reader=hf_reader,
            ))

        logger.info('Launching %d worker processes x %d readers', n_processes, readers_per_process)
        with ProcessPoolExecutor(max_workers=n_processes) as ex:
            results = list(ex.map(_run_worker, configs))
        total = sum(r for r in results if r and r > 0)
        fetch_elapsed = time.time() - start
        logger.info('All %d workers done: %d records in %.1fs (%.0f rec/s)',
                    n_processes, total, fetch_elapsed, total / fetch_elapsed if fetch_elapsed else 0)

        # Gather every rotated shard file, in shard order then sequence order within a
        # shard, and renumber into one global <prefix>-NNN.warc.gz series.
        shard_outputs = []
        for i in range(n_processes):
            shard_outputs.extend(_collect_shard_files(shards_base, f'sh{i:02d}', aws_region_name))
        n_out = len(shard_outputs)
        logger.info('Renumbering %d shard files into %d output WARCs at %s-NNN.warc.gz',
                    n_out, n_out, prefix_path)

        def _relocate(item):
            seq, src = item
            dest = _output_filename(prefix_path, sequence=seq, gzip=True)
            merge_mod.copy_object(dest, src, aws_region_name=aws_region_name)
            logger.info('Wrote %d/%d: %s', seq, n_out, dest)
            return dest

        with ThreadPoolExecutor(max_workers=max(1, min(_RENUMBER_CONCURRENCY, n_out))) as ex:
            final_paths = list(ex.map(_relocate, enumerate(shard_outputs, start=1)))

        if not keep_shards:
            merge_mod.delete_objects(shard_outputs, aws_region_name=aws_region_name)
            logger.info('Deleted %d intermediate shard files', n_out)

        logger.info('Repackage complete -> %d files %s-001..%03d.warc.gz (%.1fs total)',
                    len(final_paths), prefix_path, n_out, time.time() - start)
        return total
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
