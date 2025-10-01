import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union
from os import urandom

from botocore.exceptions import ClientError, EndpointConnectionError

from cdx_toolkit.myrequests import myrequests_get


logger = logging.getLogger(__name__)


@dataclass
class ThroughputTracker:
    """Track throughput metrics for fetchers and consumers."""

    start_time: float = 0.0
    total_bytes: int = 0
    total_requests: int = 0
    total_records: int = 0

    def start(self):
        self.start_time = time.time()

    def add(self, bytes_count: int = 0, records_count: int = 0, requests_count: int = 1):
        self.total_bytes += bytes_count
        self.total_requests += requests_count
        self.total_records += records_count

    def get_stats(self) -> dict:
        elapsed = time.time() - self.start_time

        return {
            'elapsed': elapsed,
            'total_bytes': self.total_bytes,
            'total_requests': self.total_requests,
            'total_records': self.total_records,
            'bytes_per_sec': self.total_bytes / elapsed if elapsed > 0 else 0,
            'mb_per_sec': (self.total_bytes / elapsed) / (1024 * 1024) if elapsed > 0 else 0,
            'requests_per_sec': self.total_requests / elapsed if elapsed > 0 else 0,
            'records_per_sec': self.total_records / elapsed if elapsed > 0 else 0,
        }


@dataclass(frozen=True)
class RangeJob:
    """Defines a S3 or HTTP range read request."""
    url: str
    offset: int
    length: int
    records_count: int = 1

    def is_s3(self):
        return is_s3_url(self.url)

    def get_s3_bucket_and_key(self) -> Tuple[str, str]:
        if self.is_s3():
            return parse_s3_uri(self.url)
        else:
            raise ValueError("Cannot get bucket and key from a HTTP job")


@dataclass(frozen=True)
class RangePayload:
    """Bytes output from S3 range read."""
    job: RangeJob
    data: bytes


def _backoff(attempt: int, base_backoff_seconds: float) -> float:
    """Time to sleep based on number of attempts"""
    base = base_backoff_seconds * (2 ** (attempt - 1))

    # Add random jitter between 80-120% of base delay
    return max(0.05, base * (0.8 + 0.4 * urandom(1)[0] / 255))


def parse_s3_uri(uri: str) -> Tuple[str, str]:
    """Parse a S3 URI and return bucket and prefix."""
    if not uri.startswith('s3://'):
        raise ValueError(f'Not an S3 URI: {uri}')
    rest = uri[5:]
    i = rest.find('/')
    if i <= 0 or i == len(rest) - 1:
        raise ValueError(f'Malformed S3 URI: {uri}')
    return rest[:i], rest[i + 1 :]


async def with_retries(coro_factory, *, op_name: str, max_attempts: int, base_backoff_seconds: float):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await coro_factory()
        except (TimeoutError, ClientError, EndpointConnectionError) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                logger.error('%s failed after %d attempts: %r', op_name, attempt, exc)
                break
            sleep_s = _backoff(attempt, base_backoff_seconds)
            logger.warning(
                '%s failed (attempt %d/%d) – retrying in %.2fs',
                op_name,
                attempt,
                max_attempts,
                sleep_s,
            )
            await asyncio.sleep(sleep_s)
    raise last_exc


async def ranged_get_bytes(
    job: RangeJob,
    max_attempts: int,
    base_backoff_seconds: float,
    s3_client=None,
) -> bytes:
    """Ranged get request to S3 with retries and backoff or HTTP."""
    offset = job.offset
    length = job.length

    end = offset + length - 1  # inclusive

    if job.is_s3():
        # read from S3
        bucket, key = job.get_s3_bucket_and_key()
        resp = await with_retries(
            lambda: s3_client.get_object(Bucket=bucket, Key=key, Range=f'bytes={offset}-{end}'),
            op_name=f'ranged_get {bucket}/{key}[{offset}:{end}]',
            max_attempts=max_attempts,
            base_backoff_seconds=base_backoff_seconds,
        )
        return await resp['Body'].read()

    else:
        # read from HTTP
        headers = {'Range': 'bytes={}-{}'.format(offset, end)}

        resp = myrequests_get(job.url, headers=headers)
        return resp.content


async def mpu_create(
    s3,
    bucket: str,
    key: str,
    *,
    max_attempts: int,
    base_backoff_seconds: float,
):
    """Create multi part upload to S3."""
    kwargs = {'Bucket': bucket, 'Key': key}
    resp = await with_retries(
        lambda: s3.create_multipart_upload(**kwargs),
        op_name=f'create_multipart_upload {bucket}/{key}',
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return resp['UploadId']


async def mpu_upload_part(
    s3,
    bucket: str,
    key: str,
    upload_id: str,
    part_number: int,
    body: bytes,
    max_attempts: int,
    base_backoff_seconds: float,
) -> str:
    """Upload a part of a multi-part upload to S3."""
    resp = await with_retries(
        lambda: s3.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
        ),
        op_name=f'upload_part {bucket}/{key}#{part_number}',
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return resp['ETag']


async def mpu_complete(
    s3,
    bucket: str,
    key: str,
    upload_id: str,
    parts: List[Dict],
    max_attempts: int,
    base_backoff_seconds: float,
):
    """Send complete for multi-part upload."""
    await with_retries(
        lambda: s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={'Parts': parts}
        ),
        op_name=f'complete_multipart_upload {bucket}/{key}',
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )


async def mpu_abort(s3, bucket: str, key: str, upload_id: str):
    """Abort mult-part upload."""
    try:
        await s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    except Exception:
        logger.exception('Failed to abort MPU %s on %s/%s', upload_id, bucket, key)


def is_s3_url(url: str) -> bool:
    return url.startswith("s3:/")
