import fsspec
import pytest
from cdx_toolkit.filter_warc.cdx_utils import get_index_as_string_from_path, read_cdx_line, iter_cdx_index_from_path
from tests.conftest import TEST_DATA_PATH

import tempfile
import gzip
import os
from unittest.mock import patch


def test_get_index_as_string_from_path():
    cdx_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'

    index = get_index_as_string_from_path(cdx_path)

    assert len(index) == 568010


def test_get_index_as_string_from_path_with_fs():
    fs, cdx_path = fsspec.url_to_fs(TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz')

    index = get_index_as_string_from_path(cdx_path, fs)

    assert len(index) == 568010


def test_read_cdx_line_error():
    with pytest.raises(ValueError) as ec_info:
        read_cdx_line('this_is_a_bad_CDX-line', warc_download_prefix='http://')

    assert ec_info.match('Cannot parse line')


def test_iter_cdx_index_from_path_with_error():
    """Test iter_cdx_index_from_path error handling when read_cdx_line raises exception."""

    # Create a temporary CDX file with mixed valid and invalid lines
    test_cdx_content = """
org,example)/ 20240101120000 {"url": "http://example.org/", "filename": "test.warc.gz", "offset": "100", "length": "500"}
invalid_line_here_that_will_cause_error
org,test)/ 20240102130000 {"url": "http://test.org/", "filename": "test2.warc.gz", "offset": "600", "length": "300"}
another_bad_line
org,valid)/ 20240103140000 {"url": "http://valid.org/", "filename": "test3.warc.gz", "offset": "900", "length": "200"}
""".strip()

    fd, tmp_file_path = tempfile.mkstemp(suffix='.cdx.gz')
    try:
        os.close(fd)  # Close the file descriptor

        # Write gzipped CDX content
        with gzip.open(tmp_file_path, 'wt') as f:
            f.write(test_cdx_content)

        # Mock read_cdx_line to raise exception for invalid lines
        original_read_cdx_line = read_cdx_line

        def mock_read_cdx_line(line, warc_download_prefix):
            if 'invalid' in line or 'bad' in line:
                raise ValueError(f'Mock error for line: {line}')
            return original_read_cdx_line(line, warc_download_prefix)

        with patch('cdx_toolkit.filter_warc.cdx_utils.read_cdx_line', side_effect=mock_read_cdx_line):
            # Collect results from iterator
            results = list(iter_cdx_index_from_path(tmp_file_path, 'http://warc-prefix'))

            # Should have 3 valid results despite 2 invalid lines being skipped
            assert len(results) == 3

            # Verify the valid results
            assert results[0] == ('http://warc-prefix/test.warc.gz', 100, 500)
            assert results[1] == ('http://warc-prefix/test2.warc.gz', 600, 300)
            assert results[2] == ('http://warc-prefix/test3.warc.gz', 900, 200)
    finally:
        os.unlink(tmp_file_path)
