from io import BytesIO
import os
from pathlib import Path

import fsspec
from cdx_toolkit.cli import main
from cdx_toolkit.warcer_by_cdx.cdx_utils import (
    get_index_as_string_from_path,
)
from cdx_toolkit.warcer_by_cdx.fsspec_warcer import (
    generate_caputure_objects_from_index,
)
import pytest
from warcio.archiveiterator import ArchiveIterator

from conftest import requires_aws_s3

from warcio import WARCWriter

fixture_path = Path(__file__).parent / "data/warc_by_cdx"


def assert_cli_warc_by_cdx(warc_download_prefix, base_prefix, caplog, extra_args=""):
    # test cli and check output
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"

    # --write-index-as-record

    main(
        args=f"""-v --cc --limit 10  warc_by_cdx {str(index_path)} --prefix {str(base_prefix)}/TEST_warc_by_index --creator foo --operator bob --warc-download-prefix {warc_download_prefix} {extra_args}""".split()
    )

    # Check log
    assert "Limit reached" in caplog.text

    # Validate extracted WARC
    warc_filename = "TEST_warc_by_index-000000.extracted.warc.gz"
    warc_path = str(base_prefix) + "/" + warc_filename
    resource_record = None
    info_record = None
    response_records = []
    response_contents = []

    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode("utf-8")
        
            if record.rec_type == 'response':
                response_records.append(record)
                response_contents.append(record.content_stream().read().decode("utf-8", errors="ignore"))

            # if record.rec_type == 'resource':
            #     resource_record = record

    assert len(response_records) == 10, "Invalid record count"
    # assert resource_record is not None
    # assert resource_record.length == 568010

    assert "Catalogue en ligne Mission de France" in response_contents[0], "Invalid response content"
    assert "dojo/dijit/themes/tundra/tundra" in response_contents[9], "Invalid response content"
    assert info_record is not None
    assert "operator: bob" in info_record, "Invalid WARC info"
    


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3_in_parallel_aioboto3(tmpdir, caplog):
    assert_cli_warc_by_cdx("s3://commoncrawl", base_prefix="s3://commoncrawl-dev/cdx_toolkit/ci/test-outputs" + str(tmpdir), caplog=caplog, extra_args=" --parallel 3 --implementation aioboto3")



def test_warc_info():
    warc_version = "1.0"
    gzip = False
    file_handler = BytesIO()
    filename = "foo.warc"

    info = {
        "software": "pypi_cdx_toolkit/123",
        "isPartOf": "bar",
        "description": "warc extraction based on CDX generated with: xx",
        "format": "WARC file version 1.0",
    }

    writer = WARCWriter(file_handler, gzip=gzip, warc_version=warc_version)
    warcinfo = writer.create_warcinfo_record(filename, info)
    
    writer.write_record(warcinfo)

    file_value = file_handler.getvalue().decode("utf-8")

    assert "pypi_cdx_toolkit/123" in file_value
