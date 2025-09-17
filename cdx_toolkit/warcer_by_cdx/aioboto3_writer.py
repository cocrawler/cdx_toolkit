import logging
from typing import List, Dict, Optional

from cdx_toolkit.warcer_by_cdx.aioboto3_utils import (
    mpu_abort,
    mpu_complete,
    mpu_create,
    mpu_upload_part,
)

logger = logging.getLogger(__name__)


class ShardWriter:
    """Manages one MPU: buffers bytes, uploads >=5 MiB parts, completes on close."""

    def __init__(
        self,
        shard_key: str,
        dest_bucket: str,
        content_type: Optional[str],
        min_part_size: int,
        max_attempts: int,
        base_backoff_seconds: float,
    ):
        self.shard_key = shard_key
        self.dest_bucket = dest_bucket
        self.content_type = content_type
        self.min_part_size = min_part_size
        self.max_attempts = max_attempts
        self.base_backoff_seconds = base_backoff_seconds
        self.upload_id: Optional[str] = None
        self.part_number = 1
        self.parts: List[Dict] = []
        self.buffer = bytearray()

    async def start(self, s3):
        self.upload_id = await mpu_create(
            s3,
            self.dest_bucket,
            self.shard_key,
            content_type=self.content_type,
            max_attempts=self.max_attempts,
            base_backoff_seconds=self.base_backoff_seconds,
        )
        logger.info('Started MPU for %s (UploadId=%s)', self.shard_key, self.upload_id)

    async def _flush_full_parts(self, s3):
        while len(self.buffer) >= self.min_part_size:
            chunk = self.buffer[: self.min_part_size]
            del self.buffer[: self.min_part_size]
            etag = await mpu_upload_part(
                s3,
                self.dest_bucket,
                self.shard_key,
                self.upload_id,
                self.part_number,
                bytes(chunk),
                self.max_attempts,
                self.base_backoff_seconds,
            )
            self.parts.append({'PartNumber': self.part_number, 'ETag': etag})
            self.part_number += 1

    async def write(self, s3, data: bytes):
        self.buffer.extend(data)
        await self._flush_full_parts(s3)

    async def close(self, s3):
        try:
            if self.buffer:
                etag = await mpu_upload_part(
                    s3,
                    self.dest_bucket,
                    self.shard_key,
                    self.upload_id,
                    self.part_number,
                    bytes(self.buffer),
                    self.max_attempts,
                    self.base_backoff_seconds,
                )
                self.parts.append({'PartNumber': self.part_number, 'ETag': etag})
                self.part_number += 1
                self.buffer.clear()

            if self.parts:
                await mpu_complete(
                    s3,
                    self.dest_bucket,
                    self.shard_key,
                    self.upload_id,
                    self.parts,
                    self.max_attempts,
                    self.base_backoff_seconds,
                )
            logger.info('Completed MPU for %s with %d parts.', self.shard_key, len(self.parts))
        except Exception:
            logger.exception('Completing MPU failed for %s; attempting abort.', self.shard_key)
            if self.upload_id:
                await mpu_abort(s3, self.dest_bucket, self.shard_key, self.upload_id)
            raise
