import multiprocessing
import signal
import time

from unittest.mock import patch, MagicMock


from url_is_in import SURTMatcher

from cdx_toolkit.filter_cdx.cdx_filter import _filter_single_cdx_file, _filter_single_cdx_file_args, filter_cdx
from tests.conftest import TEST_DATA_PATH

fixture_path = TEST_DATA_PATH / 'filter_cdx'


def test_filter_single_file(tmpdir):
    input_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    matcher = SURTMatcher(['fr,'])
    args = dict(
        input_path=input_path,
        output_path=tmpdir + '/filter_cdx',
        matcher=matcher,
        log_every_n=10,
        limit=100,
    )
    _, _, lines_n, included_n, errors_n = _filter_single_cdx_file_args(args)

    assert included_n == 100
    assert lines_n == 100
    assert errors_n == 0


def test_filter_single_file_empty(tmpdir):
    input_path = tmpdir + '/input'
    with open(input_path, 'w') as f:
        f.write('')

    _, _, lines_n, included_n, errors_n = _filter_single_cdx_file(
        input_path=input_path,
        output_path=tmpdir + '/output',
        matcher=None,
    )
    assert lines_n == 0
    assert included_n == 0
    assert errors_n == 0


def test_filter_single_cdx_file_input_not_found(tmpdir):

    _, _, lines_n, included_n, errors_n = _filter_single_cdx_file(
        input_path=tmpdir + "/input-not-found",
        output_path=tmpdir + '/output',
        matcher=SURTMatcher([]),
    )
    assert lines_n == 0
    assert included_n == 0
    assert errors_n == 1, 'Invalid error count'


def test_filter_single_cdx_file_with_matcher_error(tmpdir):
    class MockMatcher(SURTMatcher):
        def is_in(self, surt):
            raise ValueError()

    mock_matcher = MockMatcher([])
    input_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'

    _, _, lines_n, included_n, errors_n = _filter_single_cdx_file(
        input_path=input_path,
        output_path=tmpdir + '/output',
        matcher=mock_matcher,
    )
    assert lines_n == 1140
    assert included_n == 0
    assert errors_n == 1140, 'Invalid error count'


def test_filter_cdx_error_handling(tmpdir, caplog):
    """Test filter_cdx function error handling when exceptions occur during processing."""
    import multiprocessing

    # Store original start method to restore later
    original_start_method = multiprocessing.get_start_method()

    try:
        # Force fork method for consistent behavior across platforms
        multiprocessing.set_start_method('fork', force=True)

        def mock_filter_single_file(*args, **kwargs):
            raise ValueError()

        # Create test input and output paths
        input_paths = [str(tmpdir / 'input1.cdx'), str(tmpdir / 'input2.cdx')]
        output_paths = [str(tmpdir / 'output1.cdx'), str(tmpdir / 'output2.cdx')]

        # Replace the _process_single_file function with our mock
        with patch('cdx_toolkit.filter_cdx.cdx_filter._filter_single_cdx_file', side_effect=mock_filter_single_file):
            # Test the error handling
            total_lines, total_included, total_errors = filter_cdx(
                matcher=None,
                input_paths=input_paths,
                output_paths=output_paths,
            )

            # Verify error handling results
            assert total_errors == 1, f'Should have 1 error from the first failed file, got {total_errors}'
            assert total_lines == 0, 'Should have lines from the successful file'
            assert total_included == 0, 'Should have included lines from the successful file'

            # Check that error was logged correctly
            assert 'Error during parallel processing' in caplog.text
    finally:
        # Restore original start method
        multiprocessing.set_start_method(original_start_method, force=True)


def test_filter_cdx_keyboard_interrupt_handling(tmpdir, caplog):
    """Test that filter_cdx properly handles KeyboardInterrupt and terminates the pool."""
    # Store original start method to restore later
    original_start_method = multiprocessing.get_start_method()

    try:
        # Force fork method for consistent behavior across platforms
        multiprocessing.set_start_method('fork', force=True)

        def slow_filter_single_file(*args, **kwargs):
            """Mock function that simulates a slow process that can be interrupted."""
            time.sleep(1)  # Simulate slow processing
            return args[0], args[1], 10, 5, 0  # Return some dummy stats

        # Create test input and output paths
        input_paths = [str(tmpdir / 'input1.cdx'), str(tmpdir / 'input2.cdx')]
        output_paths = [str(tmpdir / 'output1.cdx'), str(tmpdir / 'output2.cdx')]

        # Set caplog to capture INFO level messages
        caplog.set_level('INFO')

        # Mock the Pool class to allow us to verify terminate() and join() are called
        with patch('cdx_toolkit.filter_cdx.cdx_filter.Pool') as mock_pool_class:
            mock_pool = MagicMock()
            mock_pool_class.return_value = mock_pool

            # Make imap raise KeyboardInterrupt after a short delay
            def interrupt_after_delay(*args, **kwargs):
                time.sleep(0.1)  # Brief delay before interrupt
                raise KeyboardInterrupt()

            mock_pool.imap.side_effect = interrupt_after_delay

            # Test the keyboard interrupt handling
            total_lines, total_included, total_errors = filter_cdx(
                matcher=None, input_paths=input_paths, output_paths=output_paths, n_parallel=2
            )

            # Verify that pool.terminate() and pool.join() were called
            mock_pool.terminate.assert_called_once()
            mock_pool.join.assert_called()

            # Verify that the interrupt was logged
            assert 'Process interrupted by user (Ctrl+C). Terminating running tasks...' in caplog.text
            assert 'All tasks terminated.' in caplog.text

            # Verify pool cleanup in finally block
            mock_pool.close.assert_called_once()

    finally:
        # Restore original start method
        multiprocessing.set_start_method(original_start_method, force=True)
