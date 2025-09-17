import fsspec
import pytest
from cdx_toolkit.warcer_by_cdx.cdx_utils import get_index_as_string_from_path, read_cdx_line
from tests.conftest import TEST_DATA_PATH


def test_get_index_as_string_from_path():
    cdx_path = TEST_DATA_PATH / "warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz"

    index = get_index_as_string_from_path(cdx_path)

    assert len(index) == 568010


def test_get_index_as_string_from_path_with_fs():
    fs, cdx_path = fsspec.url_to_fs(TEST_DATA_PATH / "warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz")

    index = get_index_as_string_from_path(cdx_path, fs)

    assert len(index) == 568010

get_index_as_string_from_path

def test_read_cdx_line_error():
    with pytest.raises(ValueError) as ec_info:
        read_cdx_line("this_is_a_bad_CDX-line", warc_download_prefix="http://")

    assert ec_info.match("Cannot parse line")