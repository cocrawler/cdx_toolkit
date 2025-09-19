import pytest
import asyncio
from unittest.mock import AsyncMock, patch

from cdx_toolkit.warcer_by_cdx.aioboto3_writer import ShardWriter


def test_shard_writer_init():
    """Test ShardWriter initialization."""
    shard_key = 'test-shard.warc.gz'
    dest_bucket = 'test-bucket'
    content_type = 'application/gzip'
    min_part_size = 5 * 1024 * 1024  # 5 MiB
    max_attempts = 3
    base_backoff_seconds = 0.1

    writer = ShardWriter(
        shard_key=shard_key,
        dest_bucket=dest_bucket,
        content_type=content_type,
        min_part_size=min_part_size,
        max_attempts=max_attempts,
        base_backoff_seconds=base_backoff_seconds,
    )

    assert writer.shard_key == shard_key
    assert writer.dest_bucket == dest_bucket
    assert writer.content_type == content_type
    assert writer.min_part_size == min_part_size
    assert writer.max_attempts == max_attempts
    assert writer.base_backoff_seconds == base_backoff_seconds
    assert writer.upload_id is None
    assert writer.part_number == 1
    assert writer.parts == []
    assert isinstance(writer.buffer, bytearray)
    assert len(writer.buffer) == 0


def test_shard_writer_start():
    """Test ShardWriter start method."""

    async def run_test():
        with patch('cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_create') as mock_mpu_create:
            mock_mpu_create.return_value = 'test-upload-id'

            writer = ShardWriter(
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1024,
                max_attempts=3,
                base_backoff_seconds=0.1,
            )

            mock_s3 = AsyncMock()
            await writer.start(mock_s3)

            assert writer.upload_id == 'test-upload-id'
            mock_mpu_create.assert_called_once_with(
                mock_s3,
                'test-bucket',
                'test.warc.gz',
                max_attempts=3,
                base_backoff_seconds=0.1,
            )

    asyncio.run(run_test())


def test_shard_writer_write_small_data():
    """Test ShardWriter write method with small data that stays in buffer."""

    async def run_test():
        writer = ShardWriter(
            shard_key='test.warc.gz',
            dest_bucket='test-bucket',
            content_type='application/gzip',
            min_part_size=1024,  # 1 KiB
            max_attempts=3,
            base_backoff_seconds=0.1,
        )

        mock_s3 = AsyncMock()
        small_data = b'small test data'

        await writer.write(mock_s3, small_data)

        # Data should be in buffer, no parts uploaded yet
        assert len(writer.buffer) == len(small_data)
        assert bytes(writer.buffer) == small_data
        assert writer.part_number == 1
        assert len(writer.parts) == 0

    asyncio.run(run_test())


def test_shard_writer_write_large_data():
    """Test ShardWriter write method with large data that triggers part uploads."""

    async def run_test():
        with patch('cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_upload_part') as mock_upload_part:
            mock_upload_part.return_value = 'test-etag-1'

            writer = ShardWriter(
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=100,  # 100 bytes
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            mock_s3 = AsyncMock()
            large_data = b'x' * 250  # 250 bytes, should create 2 parts

            await writer.write(mock_s3, large_data)

            # Should have uploaded 2 parts (100 bytes each) with 50 bytes remaining in buffer
            assert mock_upload_part.call_count == 2
            assert len(writer.parts) == 2
            assert writer.part_number == 3  # Next part would be #3
            assert len(writer.buffer) == 50  # Remaining bytes
            assert bytes(writer.buffer) == b'x' * 50

            # Verify parts structure
            assert writer.parts[0] == {'PartNumber': 1, 'ETag': 'test-etag-1'}
            assert writer.parts[1] == {'PartNumber': 2, 'ETag': 'test-etag-1'}

    asyncio.run(run_test())


def test_shard_writer_flush_full_parts():
    """Test ShardWriter _flush_full_parts private method directly."""

    async def run_test():
        with patch('cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_upload_part') as mock_upload_part:
            mock_upload_part.return_value = 'test-etag-flush'

            writer = ShardWriter(
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=50,  # 50 bytes
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            # Pre-fill buffer with 150 bytes (should create 3 parts of 50 bytes each)
            writer.buffer.extend(b'a' * 150)

            mock_s3 = AsyncMock()
            await writer._flush_full_parts(mock_s3)

            # Should have uploaded 3 full parts, no remainder
            assert mock_upload_part.call_count == 3
            assert len(writer.parts) == 3
            assert writer.part_number == 4  # Next part would be #4
            assert len(writer.buffer) == 0  # All data flushed

            # Verify all parts were created correctly
            for i in range(3):
                assert writer.parts[i] == {'PartNumber': i + 1, 'ETag': 'test-etag-flush'}

    asyncio.run(run_test())


def test_shard_writer_close_with_buffer():
    """Test ShardWriter close method with data remaining in buffer."""

    async def run_test():
        with patch('cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_upload_part') as mock_upload_part, patch(
            'cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_complete'
        ) as mock_complete:
            mock_upload_part.return_value = 'final-etag'

            writer = ShardWriter(
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1000,  # Large min size to keep data in buffer
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            # Add some data to buffer
            remaining_data = b'final chunk data'
            writer.buffer.extend(remaining_data)

            mock_s3 = AsyncMock()
            await writer.close(mock_s3)

            # Should upload the final part and complete MPU
            mock_upload_part.assert_called_once_with(
                mock_s3,
                'test-bucket',
                'test.warc.gz',
                'test-upload-id',
                1,  # part number
                remaining_data,
                3,  # max attempts
                0.1,  # base backoff
            )

            mock_complete.assert_called_once_with(
                mock_s3,
                'test-bucket',
                'test.warc.gz',
                'test-upload-id',
                [{'PartNumber': 1, 'ETag': 'final-etag'}],
                3,  # max attempts
                0.1,  # base backoff
            )

            # Buffer should be cleared
            assert len(writer.buffer) == 0
            assert len(writer.parts) == 1

    asyncio.run(run_test())


def test_shard_writer_close_empty():
    """Test ShardWriter close method with no data (empty buffer, no parts)."""

    async def run_test():
        with patch('cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_upload_part') as mock_upload_part, patch(
            'cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_complete'
        ) as mock_complete:
            writer = ShardWriter(
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1000,
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            # No data in buffer, no parts uploaded
            mock_s3 = AsyncMock()
            await writer.close(mock_s3)

            # Should not upload any parts or complete MPU since there's no data
            mock_upload_part.assert_not_called()
            mock_complete.assert_not_called()

            # State should remain unchanged
            assert len(writer.buffer) == 0
            assert len(writer.parts) == 0

    asyncio.run(run_test())


def test_shard_writer_close_with_exception():
    """Test ShardWriter close method with exception and abort handling."""

    async def run_test():
        with patch('cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_upload_part') as mock_upload_part, patch(
            'cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_complete'
        ) as mock_complete, patch('cdx_toolkit.warcer_by_cdx.aioboto3_writer.mpu_abort') as mock_abort:
            mock_upload_part.return_value = 'error-etag'
            mock_complete.side_effect = Exception('Complete failed')

            writer = ShardWriter(
                shard_key='test.warc.gz',
                dest_bucket='test-bucket',
                content_type='application/gzip',
                min_part_size=1000,
                max_attempts=3,
                base_backoff_seconds=0.1,
            )
            writer.upload_id = 'test-upload-id'

            # Add some data to buffer to trigger upload and complete
            writer.buffer.extend(b'some data')

            mock_s3 = AsyncMock()

            # Should raise the exception after attempting abort
            with pytest.raises(Exception, match='Complete failed'):
                await writer.close(mock_s3)

            # Should have attempted to upload part and complete, then abort on failure
            mock_upload_part.assert_called_once()
            mock_complete.assert_called_once()
            mock_abort.assert_called_once_with(mock_s3, 'test-bucket', 'test.warc.gz', 'test-upload-id')

    asyncio.run(run_test())
