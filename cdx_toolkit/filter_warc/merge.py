"""Merge several WARC shard objects into one, preserving order.

A WARC file is a concatenation of independent gzip members, so byte-concatenating
shard ``*.warc.gz`` files in order yields a single valid multi-member ``.warc.gz``.
For S3 destinations this is done server-side with multipart ``UploadPartCopy`` (the
bytes never leave S3); for local destinations the shard files are streamed together.

The S3 merge issues every ``UploadPartCopy`` **concurrently** across one multipart
upload. A single server-side copy runs at only ~20 MB/s, so a sequential 16-shard,
10 GiB merge took ~9 min (it dominated end-to-end wall, dwarfing the fetch); fanning
the copies out collapses that to roughly the slowest single part. Each shard is also
split into ``_PART_TARGET``-sized ranged parts -- this raises concurrency further and,
crucially, keeps every part under the 5 GiB ``UploadPartCopy`` ceiling so shards
larger than 5 GiB (a 100 GiB job has ~6 GiB shards) merge correctly instead of failing.

The caller is responsible for ensuring exactly one shard carries the ``warcinfo``
record (the first one) so the merged file has a single canonical warcinfo.
"""
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor

import boto3
import fsspec

from cdx_toolkit.filter_warc.s3_utils import is_s3_url, parse_s3_uri

logger = logging.getLogger(__name__)

# S3 multipart copy requires every part except the last to be >= 5 MiB, and a single
# UploadPartCopy can copy at most 5 GiB. Target part size sits between the two so big
# shards split into several parts (more concurrency, never over the 5 GiB ceiling).
_MIN_PART = 5 * 1024 * 1024
_PART_TARGET = int(os.environ.get('CDXT_MERGE_PART_MIB', '256')) * 1024 * 1024
_MERGE_CONCURRENCY = int(os.environ.get('CDXT_MERGE_CONCURRENCY', '16'))


def merge_objects(dest: str, sources: list, aws_region_name: str = 'us-east-1') -> int:
    """Concatenate ``sources`` (in the given order) into ``dest``. Returns dest size.

    - all-S3 (dest and every source on S3): merged server-side via multipart
      ``UploadPartCopy`` -- the bytes never leave S3 (fast, in-region).
    - otherwise: streamed via fsspec, which supports the local filesystem and any
      fsspec-writable backend. (Note: repackage output itself is currently limited to
      S3 and local by the WARC writer layer, so in practice sources are S3 or local.)
    """
    if is_s3_url(dest) and all(is_s3_url(s) for s in sources):
        return _merge_s3(dest, sources, aws_region_name)
    return _merge_fsspec(dest, sources)


def _plan_parts(sources_with_sizes, part_target=_PART_TARGET):
    """Flatten (source, size) shards into ordered multipart-copy parts.

    Each shard is sliced into ``part_target``-sized byte ranges. To honour S3's rule
    that every part except the overall last must be >= 5 MiB, a shard's trailing slice
    is folded into the slice before it when it would fall below that floor (a shard's
    own last slice can be small, but only the *globally* last part may be < 5 MiB).

    Returns a list of dicts ``{src, start, end}`` with ``end`` exclusive, in final
    output order; the caller assigns 1-based PartNumbers by position.
    """
    plan = []
    for src, sz in sources_with_sizes:
        slices = [[s, min(s + part_target, sz)] for s in range(0, sz, part_target)]
        if not slices:  # zero-length shard contributes nothing
            continue
        if len(slices) >= 2 and (slices[-1][1] - slices[-1][0]) < _MIN_PART:
            slices[-2][1] = slices[-1][1]
            slices.pop()
        plan.extend({'src': src, 'start': s, 'end': e} for s, e in slices)
    return plan


def _merge_s3(dest: str, sources: list, aws_region_name: str) -> int:
    s3 = boto3.client('s3', region_name=aws_region_name)
    dest_bucket, dest_key = parse_s3_uri(dest)

    # Verify every non-final shard is large enough for UploadPartCopy; if any small
    # shard would violate the 5 MiB rule, fall back to a download+reupload merge.
    heads = [s3.head_object(Bucket=b, Key=k) for b, k in (parse_s3_uri(s) for s in sources)]
    sizes = [h['ContentLength'] for h in heads]
    if any(sz < _MIN_PART for sz in sizes[:-1]):
        logger.info('A shard is < 5 MiB; merging via download+reupload instead of UploadPartCopy')
        return _merge_s3_streaming(s3, dest_bucket, dest_key, sources)

    plan = _plan_parts(list(zip(sources, sizes)))
    n = len(plan)
    mpu = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
    upload_id = mpu['UploadId']

    def copy_part(part_number, p):
        sb, sk = parse_s3_uri(p['src'])
        resp = s3.upload_part_copy(
            Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
            PartNumber=part_number, CopySource={'Bucket': sb, 'Key': sk},
            CopySourceRange='bytes={}-{}'.format(p['start'], p['end'] - 1),
        )
        logger.info('Merged part %d/%d: %s [%d-%d)', part_number, n, p['src'], p['start'], p['end'])
        return {'PartNumber': part_number, 'ETag': resp['CopyPartResult']['ETag']}

    try:
        # The copies are independent server-side operations; run them concurrently.
        # boto3 clients are thread-safe. ThreadPoolExecutor.map preserves input order
        # and re-raises the first failure, which trips the abort below.
        with ThreadPoolExecutor(max_workers=max(1, min(_MERGE_CONCURRENCY, n))) as ex:
            parts = list(ex.map(copy_part, range(1, n + 1), plan))
        s3.complete_multipart_upload(
            Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
            MultipartUpload={'Parts': parts},
        )
    except Exception:
        logger.exception('Merge failed; aborting multipart upload for %s', dest)
        s3.abort_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id)
        raise
    size = s3.head_object(Bucket=dest_bucket, Key=dest_key)['ContentLength']
    logger.info('Merged %d shards (%d parts) -> %s (%d bytes)', len(sources), n, dest, size)
    return size


def _merge_s3_streaming(s3, dest_bucket, dest_key, sources) -> int:
    mpu = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
    upload_id = mpu['UploadId']
    parts = []
    buf = bytearray()
    part_no = 1
    try:
        def flush(final=False):
            nonlocal buf, part_no
            while len(buf) >= _MIN_PART or (final and buf):
                take = len(buf) if final else _MIN_PART
                chunk = bytes(buf[:take])
                del buf[:take]
                r = s3.upload_part(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
                                   PartNumber=part_no, Body=chunk)
                parts.append({'PartNumber': part_no, 'ETag': r['ETag']})
                part_no += 1
                if final and not buf:
                    break
        for src in sources:
            sb, sk = parse_s3_uri(src)
            body = s3.get_object(Bucket=sb, Key=sk)['Body'].read()
            buf.extend(body)
            flush()
        flush(final=True)
        s3.complete_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
                                     MultipartUpload={'Parts': parts})
    except Exception:
        s3.abort_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id)
        raise
    return s3.head_object(Bucket=dest_bucket, Key=dest_key)['ContentLength']


def _merge_fsspec(dest: str, sources: list) -> int:
    """Stream sources into dest via fsspec (local filesystem or any fsspec backend)."""
    out_fs, _ = fsspec.core.url_to_fs(dest)
    with fsspec.open(dest, 'wb') as out:
        for src in sources:
            with fsspec.open(src, 'rb') as fh:
                shutil.copyfileobj(fh, out, length=8 * 1024 * 1024)
            logger.info('Merged %s', src)
    size = out_fs.size(dest)
    logger.info('Merged %d shards -> %s (%s bytes)', len(sources), dest, size)
    return size or 0


def delete_objects(objs: list, aws_region_name: str = 'us-east-1') -> None:
    """Delete shard objects after a successful merge (S3, local, or any fsspec backend)."""
    s3 = None
    for o in objs:
        if is_s3_url(o):
            if s3 is None:
                s3 = boto3.client('s3', region_name=aws_region_name)
            b, k = parse_s3_uri(o)
            s3.delete_object(Bucket=b, Key=k)
        else:
            fs, path = fsspec.core.url_to_fs(o)
            try:
                fs.rm(path)
            except FileNotFoundError:
                pass
