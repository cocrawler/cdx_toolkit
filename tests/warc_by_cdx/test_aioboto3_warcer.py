import asyncio
from unittest.mock import patch, AsyncMock

from cdx_toolkit.warcer_by_cdx.aioboto3_warcer import filter_warc_by_cdx_via_aioboto3, get_range_jobs_from_index_paths


def test_filter_warc_by_cdx_via_aioboto3_keyboard_interrupt(caplog):
    """Test filter_warc_by_cdx_via_aioboto3 KeyboardInterrupt exception handling."""

    # Mock the async function to raise KeyboardInterrupt
    async def mock_async_function(*args, **kwargs):
        raise KeyboardInterrupt('User interrupted')

    with patch(
        'cdx_toolkit.warcer_by_cdx.aioboto3_warcer.filter_warc_by_cdx_via_aioboto3_async',
        side_effect=mock_async_function,
    ):
        # Call the function with minimal required parameters
        result = filter_warc_by_cdx_via_aioboto3(
            index_paths=['test_index.cdx'], prefix_path='s3://test-bucket/test-prefix', writer_info={'software': 'test'}
        )

        # Verify that KeyboardInterrupt was handled correctly
        assert result == -1, 'Should return -1 when KeyboardInterrupt is caught'

        # Check that the warning message was logged
        assert 'Interrupted by user.' in caplog.text

        # Verify the log level is warning
        warning_records = [record for record in caplog.records if record.levelname == 'WARNING']
        assert len(warning_records) == 1
        assert warning_records[0].message == 'Interrupted by user.'



def test_get_range_jobs_from_index_paths_exception_handling_with_logging(caplog):
    """Test get_range_jobs_from_index_paths logs errors when iter_cdx_index_from_path raises."""

    async def run_test():
        # Create a mock queue
        key_queue = AsyncMock(spec=asyncio.Queue)

        # Test parameters
        index_paths = ['failing_index.cdx']
        warc_download_prefix = 'http://test-prefix'
        num_fetchers = 1

        # Mock iter_cdx_index_from_path to always raise exception
        def mock_iter_cdx_index_from_path(index_path, warc_download_prefix):
            raise ValueError('Simulated CDX parsing error')

        with patch(
            'cdx_toolkit.warcer_by_cdx.aioboto3_warcer.iter_cdx_index_from_path',
            side_effect=mock_iter_cdx_index_from_path,
        ):
            # Run the function
            await get_range_jobs_from_index_paths(
                key_queue=key_queue,
                index_paths=index_paths,
                warc_download_prefix=warc_download_prefix,
                num_fetchers=num_fetchers,
                limit=0,
            )

            # Verify error was logged
            assert 'Failed to read CDX index from failing_index.cdx' in caplog.text
            assert 'Simulated CDX parsing error' in caplog.text

            # Verify that only STOP signal was sent (no jobs due to exception)
            assert key_queue.put.call_count == 1  # Only 1 STOP signal

    # Run the test
    asyncio.run(run_test())
