import os
from typing import List, Optional

import fsspec
from cdx_toolkit.cli import main

import pytest
from warcio.archiveiterator import ArchiveIterator

from tests.conftest import requires_aws_s3, TEST_DATA_PATH


fixture_path = TEST_DATA_PATH / 'warc_by_cdx'


def assert_cli_warc_by_cdx(
    warc_download_prefix,
    base_prefix,
    caplog,
    extra_args: Optional[List[str]] = None,
    # warc_filename: str = 'TEST_warc_by_index-000000.extracted.warc.gz',
    warc_filename: str = 'TEST_warc_by_index-000000-001.extracted.warc.gz',  # due to parallel writer
):
    # test cli and check output
    index_path = fixture_path / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'
    resource_record_path = TEST_DATA_PATH / 'filter_cdx/whitelist_10_urls.txt'

    base_prefix = str(base_prefix)

    if extra_args is None:
        extra_args = []

    main(
        args=[
            '-v',
            '--limit=10',
            'warc_by_cdx',
            f'--cdx-path={str(index_path)}',
            '--write-paths-as-resource-records',
            str(resource_record_path),
            f'--prefix={base_prefix}/TEST_warc_by_index',
            '--creator=foo',
            '--operator=bob',
            f'--warc-download-prefix={warc_download_prefix}',
        ]
        + extra_args
    )

    # Check log
    assert 'Limit reached' in caplog.text

    # Validate extracted WARC
    if 's3:' in base_prefix:
        warc_path = base_prefix + '/' + warc_filename
    else:
        warc_path = os.path.join(base_prefix, warc_filename)

    info_record = None
    response_records = []
    response_contents = []

    resource_record = None
    resource_record_content = None

    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode('utf-8')

            if record.rec_type == 'response':
                response_records.append(record)
                response_contents.append(record.content_stream().read().decode('utf-8', errors='ignore'))

            if record.rec_type == 'resource':
                resource_record = record
                resource_record_content = record.content_stream().read().decode('utf-8')

    assert len(response_records) == 10, 'Invalid record count'

    assert info_record is not None, 'Invalid info record'
    assert 'operator: bob' in info_record, 'Invalid info record'

    assert 'Catalogue en ligne Mission de France' in response_contents[0], 'Invalid response content'
    assert 'dojo/dijit/themes/tundra/tundra' in response_contents[9], 'Invalid response content'

    assert resource_record is not None, 'Resource record not set'

    assert resource_record_content[:10] == 'example.co', 'Invalid resource record'

    # Disabled due to OS-specific line endings
    # assert resource_record_content[-20:-1] == 'hr.fr/produit/t-837', 'Invalid resource record'

    # Calculate expected length based on the actual source file on current OS
    with open(resource_record_path, 'rb') as f:
        expected_length = len(f.read())

    assert resource_record.length == expected_length, (
        f'Invalid resource record length {resource_record.length}, expected {expected_length} '
        f'(computed from {resource_record_path} on current OS)'
    )


def test_cli_warc_by_cdx_over_http(tmpdir, caplog):
    assert_cli_warc_by_cdx('https://data.commoncrawl.org', base_prefix=tmpdir, caplog=caplog)


def test_cli_warc_by_cdx_over_http_in_parallel(tmpdir, caplog):
    assert_cli_warc_by_cdx(
        'https://data.commoncrawl.org', base_prefix=tmpdir, caplog=caplog, extra_args=['--parallel=3']
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3(tmpdir, caplog):
    assert_cli_warc_by_cdx('s3://commoncrawl', base_prefix=tmpdir, caplog=caplog)


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=s3_tmpdir,
        caplog=caplog,
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3_in_parallel(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=s3_tmpdir,
        caplog=caplog,
        extra_args=['--parallel=3', '--is-part-of=foobar'],
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_s3_warc_filter(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=s3_tmpdir,
        caplog=caplog,
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_http_to_s3_in_parallel(s3_tmpdir, caplog):
    assert_cli_warc_by_cdx(
        'https://data.commoncrawl.org',
        base_prefix=s3_tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
        ],
    )


@requires_aws_s3
def test_cli_warc_by_cdx_over_s3_to_local_in_parallel(tmpdir, caplog):
    assert_cli_warc_by_cdx(
        's3://commoncrawl',
        base_prefix=tmpdir,
        caplog=caplog,
        extra_args=[
            '--parallel=3',
        ],
    )


def test_warc_by_cdx_no_index_files_found_exits(tmpdir, caplog):
    # Test that warc_by_cdx exits when no index files match the glob pattern
    with pytest.raises(SystemExit) as exc_info:
        main(
            args=[
                '-v',
                'warc_by_cdx',
                f'--cdx-path={str(tmpdir)}',
                f'--prefix={str(tmpdir)}/TEST',
                '--cdx-glob=/nonexistent-pattern-*.gz',
            ]
        )

    assert exc_info.value.code == 1
    assert 'no index files found' in caplog.text


def test_warc_by_cdx_subprefix_and_metadata(tmpdir):
    # Test subprefix functionality and creator/operator metadata
    index_path = fixture_path / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'

    main(
        args=[
            '-v',
            '--limit=1',
            'warc_by_cdx',
            f'--cdx-path={str(index_path)}',
            f'--prefix={str(tmpdir)}/TEST',
            '--subprefix=SUB',
            '--creator=test_creator',
            '--operator=test_operator',
        ]
    )

    # Check that WARC file was created with subprefix
    warc_path = os.path.join(tmpdir, 'TEST-SUB-000000-001.extracted.warc.gz')
    assert os.path.exists(warc_path)

    # Validate metadata in warcinfo record
    info_record = None
    with open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode('utf-8')
                break

    assert info_record is not None
    assert 'creator: test_creator' in info_record
    assert 'operator: test_operator' in info_record


def test_warc_by_cdx_without_creator_operator(tmpdir):
    # Test that creator and operator are optional (lines 44-47)
    index_path = fixture_path / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'

    main(
        args=[
            '-v',
            '--limit=1',
            'warc_by_cdx',
            f'--cdx-path={str(index_path)}',
            f'--prefix={str(tmpdir)}/TEST_NO_META',
        ]
    )

    # Check that WARC file was created
    warc_path = os.path.join(tmpdir, 'TEST_NO_META-000000-001.extracted.warc.gz')
    assert os.path.exists(warc_path)

    # Validate that creator/operator are not in warcinfo record
    info_record = None
    with open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode('utf-8')
                break

    assert info_record is not None
    assert 'creator:' not in info_record
    assert 'operator:' not in info_record


def test_resource_records_paths_mismatch():
    # Test if mismatch of number of paths for resource records and their metdata is raised.
    with pytest.raises(ValueError) as exc_info:
        main(
            args=[
                '-v',
                'warc_by_cdx',
                '--cdx-path=foo/bar',
                '--write-paths-as-resource-records',
                'resource1',
                'resource2',
                '--write-paths-as-resource-records-metadata',
                'metadata2',
            ]
        )
    assert exc_info.match('Number of paths to resource records')


def test_metadata_paths_without_resource_records_paths():
    # Test if error of missing resource records paths is raised.
    with pytest.raises(ValueError) as exc_info:
        main(
            args=['-v', 'warc_by_cdx', '--cdx-path=foo/bar', '--write-paths-as-resource-records-metadata', 'metadata2']
        )
    assert exc_info.match('Metadata paths are set but')


def test_cli_warc_by_athena(
    tmpdir,
    caplog,
):
    base_prefix = tmpdir
    warc_download_prefix = 's3://commoncrawl'
    extra_args: Optional[List[str]] = None
    warc_filename: str = 'TEST_warc_by_index-000000-001.extracted.warc.gz'  # due to parallel writer
    base_prefix = str(base_prefix)

    if extra_args is None:
        extra_args = []

    main(
        args=[
            '-v',
            '--limit=10',
            'warc_by_cdx',
            '--target-source=athena',
            '--athena-database=ccindex',
            '--athena-s3-output=s3://commoncrawl-ci-temp/athena-results/',
            '--athena-hostnames',
            'oceancolor.sci.gsfc.nasa.gov',
            'example.com',
            f'--prefix={base_prefix}/TEST_warc_by_index',
            '--creator=foo',
            '--operator=bob',
            f'--warc-download-prefix={warc_download_prefix}',
        ]
        + extra_args
    )

    # Check log
    assert 'WARC records extracted: 10' in caplog.text

    # Validate extracted WARC
    if 's3:' in base_prefix:
        warc_path = base_prefix + '/' + warc_filename
    else:
        warc_path = os.path.join(base_prefix, warc_filename)

    info_record = None
    response_records = []
    response_contents = []

    # resource_record = None
    # resource_record_content = None

    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode('utf-8')

            if record.rec_type == 'response':
                response_records.append(record)
                response_contents.append(record.content_stream().read().decode('utf-8', errors='ignore'))

            # if record.rec_type == 'resource':
            #     resource_record = record
            #     resource_record_content = record.content_stream().read().decode('utf-8')

    assert len(response_records) == 10, 'Invalid record count'

    assert info_record is not None, 'Invalid info record'
    assert 'operator: bob' in info_record, 'Invalid info record'

    assert '<h1>Example Domain</h1>' in response_contents[0], 'Invalid response content'
    assert '<h1>Example Domain</h1>' in response_contents[9], 'Invalid response content'
