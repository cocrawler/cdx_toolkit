"""Multi-process repackage: shard the range jobs, fetch each shard in its own process
(one asyncio event loop per CPU core), then merge the shards into a single WARC.

A single asyncio loop saturates one CPU core on many small range-GETs, so reaching
all cores needs multiple processes. This orchestrator keeps that entirely inside
``cdxt repackage`` (no external driver):

1. Drain the configured source once and split it into N self-contained shard CSVs,
   hashed by WARC filename so every record of a file stays in one shard.
2. Run one worker process per shard concurrently. Each writes exactly one output WARC
   (``<prefix>-shNN-000000-001.warc.gz``). Only shard 0 writes the ``warcinfo`` record;
   all shards share one canonical ``WARC-Record-ID`` and the warcinfo ``filename`` field
   names the final merged file.
3. Merge the shard objects in order into ``<prefix>.warc.gz`` (server-side on S3), then
   delete the shards.
"""
import hashlib
import logging
import os
import shutil
import tempfile
import time
import uuid
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List, Optional

from cdx_toolkit.filter_warc.sources.csv import CsvSource, RangeJobCsvWriter
from cdx_toolkit.filter_warc.warc_filter import WARCFilter
from cdx_toolkit.filter_warc.warc_utils import generate_warc_filename
from cdx_toolkit.filter_warc.s3_utils import is_s3_url, parse_s3_uri
from cdx_toolkit.filter_warc import merge as merge_mod

logger = logging.getLogger(__name__)


def _shard_filename(prefix_path: str, subprefix: str, gzip: bool = True) -> str:
    """Object path a worker writes (sequence 1, no rotation)."""
    if is_s3_url(prefix_path):
        bucket, key_prefix = parse_s3_uri(prefix_path)
        key = generate_warc_filename(key_prefix, sequence=1, writer_subprefix=subprefix, gzip=gzip)
        return f's3://{bucket}/{key}'
    return generate_warc_filename(prefix_path, sequence=1, writer_subprefix=subprefix, gzip=gzip)


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
        # still needed so WARCFilter.needs_aws() knows reads come from S3.
        warc_download_prefix=cfg['warc_download_prefix'],
        n_parallel_readers=cfg['readers'],
        aws_region_name=cfg['aws_region_name'],
        max_attempts=cfg['max_attempts'],
        max_file_size=None,  # never rotate: exactly one file per worker
        warcinfo_record_id=cfg['warcinfo_record_id'],
        warcinfo_filename=cfg['warcinfo_filename'],
        write_warcinfo=cfg['write_warcinfo'],
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
    warc_download_prefix: Optional[str] = None,
    keep_shards: bool = False,
) -> int:
    """Shard -> N worker processes -> merge into a single <prefix>.warc.gz. Returns count."""
    if writer_subprefix:
        logger.warning('--subprefix is ignored in multi-process mode (shards use shNN)')

    final_dest = prefix_path + '.warc.gz'
    final_name = (parse_s3_uri(final_dest)[1] if is_s3_url(final_dest) else final_dest).split('/')[-1]
    warcinfo_id = f'<urn:uuid:{uuid.uuid4()}>'

    tmp_dir = tempfile.mkdtemp(prefix='cdxt_shards_')
    start = time.time()
    try:
        shard_csvs = _split_source_to_shards(source, n_processes, tmp_dir, record_limit)

        configs = []
        shard_outputs = []
        for i in range(n_processes):
            subprefix = f'sh{i:02d}'
            shard_outputs.append(_shard_filename(prefix_path, subprefix))
            configs.append(dict(
                shard_csv=shard_csvs[i],
                prefix_path=prefix_path,
                subprefix=subprefix,
                writer_info=writer_info,
                write_paths_as_metadata_records=write_paths_as_metadata_records,
                log_every_n=log_every_n,
                readers=readers_per_process,
                aws_region_name=aws_region_name,
                max_attempts=max_attempts,
                warcinfo_record_id=warcinfo_id,
                warcinfo_filename=final_name,
                write_warcinfo=(i == 0),  # only the first shard carries the warcinfo
                uvloop=uvloop,
                warc_download_prefix=warc_download_prefix,
            ))

        logger.info('Launching %d worker processes x %d readers', n_processes, readers_per_process)
        with ProcessPoolExecutor(max_workers=n_processes) as ex:
            results = list(ex.map(_run_worker, configs))
        total = sum(r for r in results if r and r > 0)
        fetch_elapsed = time.time() - start
        logger.info('All %d workers done: %d records in %.1fs (%.0f rec/s)',
                    n_processes, total, fetch_elapsed, total / fetch_elapsed if fetch_elapsed else 0)

        logger.info('Merging %d shards -> %s', n_processes, final_dest)
        merge_mod.merge_objects(final_dest, shard_outputs, aws_region_name=aws_region_name)

        if not keep_shards:
            merge_mod.delete_objects(shard_outputs, aws_region_name=aws_region_name)
            logger.info('Deleted %d intermediate shards', n_processes)

        logger.info('Repackage complete -> %s (%.1fs total)', final_dest, time.time() - start)
        return total
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
