import pytest
from cdx_toolkit.filter_warc.warc_utils import get_resource_record_from_path
from tests.conftest import TEST_DATA_PATH


def test_get_resource_record_from_path():
    resource_path = TEST_DATA_PATH / 'filter_cdx/whitelist_10_urls.txt'
    record = get_resource_record_from_path(resource_path, warcinfo_id="abc123")

    assert record.content_type == 'text/plain'

    record_headers = dict(record.rec_headers.headers)
    assert record_headers['WARC-Target-URI'] == str(resource_path)
    assert record_headers["WARC-Warcinfo-ID"] == "abc123"


def test_get_resource_record_from_path_with_metadata():
    resource_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    metadata_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.metadata.json'

    record = get_resource_record_from_path(resource_path, metadata_path=metadata_path, warcinfo_id="abc123")

    assert record.content_type == 'application/cdx'

    record_headers = dict(record.rec_headers.headers)
    assert record_headers['WARC-Target-URI'] == 'filter_cdx.cdx.gz'
    assert record_headers["WARC-Warcinfo-ID"] == "abc123"


def test_get_resource_record_from_path_with_invalid_metadata_path():
    with pytest.raises(ValueError):
        resource_path = TEST_DATA_PATH / 'filter_cdx/whitelist_10_urls.txt'
        get_resource_record_from_path(resource_path, metadata_path='invalid_metadata.xy', warcinfo_id="abc123")
