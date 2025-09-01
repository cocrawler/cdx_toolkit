import asyncio
import logging
import time
from dataclasses import dataclass

import logging

from botocore.exceptions import ClientError, EndpointConnectionError

_STOP = object()

logger = logging.getLogger(__name__)


@dataclass
class ThroughputTracker:
    """Track throughput metrics for fetchers and consumers."""

    start_time: float = 0.0
    total_bytes: int = 0
    total_requests: int = 0

    def start(self):
        self.start_time = time.time()

    def add_bytes(self, bytes_count: int):
        self.total_bytes += bytes_count
        self.total_requests += 1

    def get_stats(self) -> dict:
        elapsed = time.time() - self.start_time
        if elapsed <= 0:
            return {
                "elapsed": 0,
                "bytes_per_sec": 0,
                "mb_per_sec": 0,
                "requests_per_sec": 0,
            }

        return {
            "elapsed": elapsed,
            "total_bytes": self.total_bytes,
            "total_requests": self.total_requests,
            "bytes_per_sec": self.total_bytes / elapsed,
            "mb_per_sec": (self.total_bytes / elapsed) / (1024 * 1024),
            "requests_per_sec": self.total_requests / elapsed,
        }


@dataclass(frozen=True)
class RangeJob:
    bucket: str
    key: str
    offset: int
    length: int


@dataclass(frozen=True)
class RangePayload:
    job: RangeJob
    data: bytes


def _backoff(attempt: int, base_backoff_seconds: float) -> float:
    base = base_backoff_seconds * (2 ** (attempt - 1))
    # jitter ±20%
    import os as _os

    return max(0.05, base * (0.8 + 0.4 * _os.urandom(1)[0] / 255))


def parse_s3_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Not an S3 URI: {uri}")
    rest = uri[5:]
    i = rest.find("/")
    if i <= 0 or i == len(rest) - 1:
        raise ValueError(f"Malformed S3 URI: {uri}")
    return rest[:i], rest[i + 1 :]


async def with_retries(
    coro_factory, *, op_name: str, max_attempts: int, base_backoff_seconds: float
):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await coro_factory()
        except (TimeoutError, ClientError, EndpointConnectionError) as exc:
            last_exc = exc
            if attempt >= max_attempts:
                logger.error("%s failed after %d attempts: %r", op_name, attempt, exc)
                break
            sleep_s = _backoff(attempt, base_backoff_seconds)
            logger.warning(
                "%s failed (attempt %d/%d) – retrying in %.2fs",
                op_name,
                attempt,
                max_attempts,
                sleep_s,
            )
            await asyncio.sleep(sleep_s)
    raise last_exc


async def get_object_stream(
    s3, bucket: str, key: str, max_attempts: int, base_backoff_seconds: float
):
    resp = await with_retries(
        lambda: s3.get_object(Bucket=bucket, Key=key),
        op_name=f"get_object {bucket}/{key}",
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return resp["Body"]


async def ranged_get_bytes(
    s3,
    bucket: str,
    key: str,
    offset: int,
    length: int,
    max_attempts: int,
    base_backoff_seconds: float,
) -> bytes:
    end = offset + length - 1  # inclusive
    resp = await with_retries(
        lambda: s3.get_object(Bucket=bucket, Key=key, Range=f"bytes={offset}-{end}"),
        op_name=f"ranged_get {bucket}/{key}[{offset}:{end}]",
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return await resp["Body"].read()


async def mpu_create(
    s3,
    bucket: str,
    key: str,
    *,
    content_type: str | None,
    max_attempts: int,
    base_backoff_seconds: float,
):
    kwargs = {"Bucket": bucket, "Key": key}
    if content_type:
        kwargs["ContentType"] = content_type
    resp = await with_retries(
        lambda: s3.create_multipart_upload(**kwargs),
        op_name=f"create_multipart_upload {bucket}/{key}",
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return resp["UploadId"]


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
    resp = await with_retries(
        lambda: s3.upload_part(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            PartNumber=part_number,
            Body=body,
        ),
        op_name=f"upload_part {bucket}/{key}#{part_number}",
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )
    return resp["ETag"]


async def mpu_complete(
    s3,
    bucket: str,
    key: str,
    upload_id: str,
    parts: list[dict],
    max_attempts: int,
    base_backoff_seconds: float,
):
    await with_retries(
        lambda: s3.complete_multipart_upload(
            Bucket=bucket, Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        ),
        op_name=f"complete_multipart_upload {bucket}/{key}",
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )


async def mpu_abort(s3, bucket: str, key: str, upload_id: str):
    try:
        await s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
    except Exception:
        logger.exception("Failed to abort MPU %s on %s/%s", upload_id, bucket, key)
