"""Merge several WARC shard objects into one, preserving order.

A WARC file is a concatenation of independent gzip members, so byte-concatenating
shard ``*.warc.gz`` files in order yields a single valid multi-member ``.warc.gz``.
For S3 destinations this is done server-side with multipart ``UploadPartCopy`` (the
bytes never leave S3); for local destinations the shard files are streamed together.

The caller is responsible for ensuring exactly one shard carries the ``warcinfo``
record (the first one) so the merged file has a single canonical warcinfo.
"""
import logging
import shutil

import boto3
import fsspec

from cdx_toolkit.filter_warc.s3_utils import is_s3_url, parse_s3_uri

logger = logging.getLogger(__name__)

# S3 multipart copy requires every part except the last to be >= 5 MiB.
_MIN_PART = 5 * 1024 * 1024


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


def _merge_s3(dest: str, sources: list, aws_region_name: str) -> int:
    s3 = boto3.client('s3', region_name=aws_region_name)
    dest_bucket, dest_key = parse_s3_uri(dest)

    # Verify every non-final part is large enough for UploadPartCopy; if any small
    # shard would violate the 5 MiB rule, fall back to a download+reupload merge.
    heads = [s3.head_object(Bucket=b, Key=k) for b, k in (parse_s3_uri(s) for s in sources)]
    sizes = [h['ContentLength'] for h in heads]
    if any(sz < _MIN_PART for sz in sizes[:-1]):
        logger.info('A shard is < 5 MiB; merging via download+reupload instead of UploadPartCopy')
        return _merge_s3_streaming(s3, dest_bucket, dest_key, sources)

    mpu = s3.create_multipart_upload(Bucket=dest_bucket, Key=dest_key)
    upload_id = mpu['UploadId']
    parts = []
    try:
        for i, src in enumerate(sources, start=1):
            sb, sk = parse_s3_uri(src)
            resp = s3.upload_part_copy(
                Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
                PartNumber=i, CopySource={'Bucket': sb, 'Key': sk},
            )
            parts.append({'PartNumber': i, 'ETag': resp['CopyPartResult']['ETag']})
            logger.info('Merged part %d/%d: %s', i, len(sources), src)
        s3.complete_multipart_upload(
            Bucket=dest_bucket, Key=dest_key, UploadId=upload_id,
            MultipartUpload={'Parts': parts},
        )
    except Exception:
        logger.exception('Merge failed; aborting multipart upload for %s', dest)
        s3.abort_multipart_upload(Bucket=dest_bucket, Key=dest_key, UploadId=upload_id)
        raise
    size = s3.head_object(Bucket=dest_bucket, Key=dest_key)['ContentLength']
    logger.info('Merged %d shards -> %s (%d bytes)', len(sources), dest, size)
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
