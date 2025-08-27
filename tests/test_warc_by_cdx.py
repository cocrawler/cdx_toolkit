import os
from pathlib import Path

import fsspec
from cdx_toolkit.cli import main
from cdx_toolkit.warcer_by_cdx import (
    generate_caputure_objects_from_index,
    get_index_from_path,
)
import pytest
from warcio.archiveiterator import ArchiveIterator

from conftest import requires_aws_s3


fixture_path = Path(__file__).parent / "data/warc_by_cdx"


def assert_cli_warc_by_cdx(warc_download_prefix, base_prefix, caplog, extra_args=""):
    # test cli and check output
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"

    main(
        args=f"""-v --cc --limit 10  warc_by_cdx {str(index_path)} --write-index-as-record --prefix {str(base_prefix)}/TEST_warc_by_index --creator foo --operator bob --warc-download-prefix {warc_download_prefix} {extra_args}""".split()
    )

    # Check log
    assert "Limit reached" in caplog.text

    # Validate extracted WARC
    warc_filename = "TEST_warc_by_index-000000.extracted.warc.gz"
    warc_path = str(base_prefix) + "/" + warc_filename
    resource_record = None
    info_record = None
    response_records = []

    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode("utf-8")
        
            if record.rec_type == 'response':
                response_records.append(record)
                
            if record.rec_type == 'resource':
                resource_record = record

    assert len(response_records) == 10
    assert resource_record is not None
    assert resource_record.length == 568010

    assert info_record is not None
    assert "operator: bob" in info_record


def test_cli_warc_by_cdx_over_http(tmpdir, caplog):
    assert_cli_warc_by_cdx("https://data.commoncrawl.org", base_prefix=tmpdir, caplog=caplog)

def test_cli_warc_by_cdx_over_http_in_parallel(tmpdir, caplog):
    assert_cli_warc_by_cdx("https://data.commoncrawl.org", base_prefix=tmpdir, caplog=caplog, extra_args=" --parallel 3")

@requires_aws_s3
def test_cli_warc_by_cdx_over_s3(tmpdir, caplog):
    assert_cli_warc_by_cdx("s3://commoncrawl", base_prefix=tmpdir, caplog=caplog)

@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3(tmpdir, caplog):
    assert_cli_warc_by_cdx("s3://commoncrawl", base_prefix="s3://commoncrawl-dev/cdx_toolkit/ci/test-outputs" + str(tmpdir), caplog=caplog)

@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3_in_parallel(tmpdir, caplog):
    assert_cli_warc_by_cdx("s3://commoncrawl", base_prefix="s3://commoncrawl-dev/cdx_toolkit/ci/test-outputs" + str(tmpdir), caplog=caplog, extra_args=" --parallel 3")


def test_get_caputure_objects_from_index():
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"

    for obj in generate_caputure_objects_from_index(get_index_from_path(index_path)):
        break

    assert obj.data["length"] == "9754"


def test_warc_by_cdx_no_index_files_found_exits(tmpdir, caplog):
    # Test that warc_by_cdx exits when no index files match the glob pattern
    with pytest.raises(SystemExit) as exc_info:
        main(
            args=f"""-v --cc --cc-mirror https://index.commoncrawl.org/ warc_by_cdx {str(tmpdir)} --prefix {str(tmpdir)}/TEST --index-glob "/nonexistent-pattern-*.gz" """.split()
        )
    
    assert exc_info.value.code == 1
    assert "no index files found" in caplog.text


def test_generate_caputure_objects_invalid_cdx_line():    
    # Test invalid CDX line parsing (line with wrong number of columns)
    with pytest.raises(ValueError):
        list(generate_caputure_objects_from_index("invalid-format"))


def test_generate_caputure_objects_with_limit():
    # Test limit functionality in get_caputure_objects_from_index
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"
    index_content = get_index_from_path(index_path)
    
    # Count objects with limit=2
    objects = list(generate_caputure_objects_from_index(index_content, limit=2))
    
    # Should stop after 2 objects
    assert len(objects) == 2


def test_warc_by_cdx_subprefix_and_metadata(tmpdir):
    # Test subprefix functionality and creator/operator metadata
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"

    main(
        args=f"""-v --cc --cc-mirror https://index.commoncrawl.org/ --limit 1 warc_by_cdx {str(index_path)} --prefix {str(tmpdir)}/TEST --subprefix SUB --creator test_creator --operator test_operator""".split()
    )

    # Check that WARC file was created with subprefix
    warc_path = os.path.join(tmpdir, "TEST-SUB-000000.extracted.warc.gz")
    assert os.path.exists(warc_path)

    # Validate metadata in warcinfo record
    info_record = None
    with open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode("utf-8")
                break

    assert info_record is not None
    assert "creator: test_creator" in info_record
    assert "operator: test_operator" in info_record


def test_warc_by_cdx_without_creator_operator(tmpdir):
    # Test that creator and operator are optional (lines 44-47)
    index_path = fixture_path / "filtered_CC-MAIN-2024-30_cdx-00187.gz"

    main(
        args=f"""-v --cc --cc-mirror https://index.commoncrawl.org/ --limit 1 warc_by_cdx {str(index_path)} --prefix {str(tmpdir)}/TEST_NO_META""".split()
    )

    # Check that WARC file was created
    warc_path = os.path.join(tmpdir, "TEST_NO_META-000000.extracted.warc.gz")
    assert os.path.exists(warc_path)

    # Validate that creator/operator are not in warcinfo record
    info_record = None
    with open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode("utf-8")
                break

    assert info_record is not None
    assert "creator:" not in info_record
    assert "operator:" not in info_record
