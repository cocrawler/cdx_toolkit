import time
from dataclasses import dataclass

from cdx_toolkit.filter_warc.s3_utils import is_s3_url, parse_s3_uri, with_retries
from typing import Tuple

from cdx_toolkit.myrequests import myrequests_get



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
            raise ValueError('Cannot get bucket and key from a HTTP job')

    async def ranged_get_bytes(
        self,
        max_attempts: int,
        base_backoff_seconds: float,
        s3_client=None,
    ) -> bytes:
        """Ranged get request to S3 with retries and backoff or HTTP."""
        offset = self.offset
        length = self.length

        end = offset + length - 1  # inclusive

        if self.is_s3():
            # read from S3
            bucket, key = self.get_s3_bucket_and_key()
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

            resp = myrequests_get(self.url, headers=headers)
            return resp.content


@dataclass(frozen=True)
class RangePayload:
    """Bytes output from S3 or HTTP range read."""

    job: RangeJob
    data: bytes