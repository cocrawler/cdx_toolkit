import os
from typing import List, Optional

import fsspec
from cdx_toolkit.cli import main

import pytest
from warcio.archiveiterator import ArchiveIterator

from tests.conftest import requires_aws_athena, requires_aws_s3, TEST_DATA_PATH


fixture_path = TEST_DATA_PATH / 'warc_by_cdx'


def assert_cli_warc_by_cdx(
    warc_download_prefix,
    base_prefix,
    caplog,
    extra_args: Optional[List[str]] = None,
    # warc_filename: str = 'TEST_warc_by_index-000000.warc.gz',
    warc_filename: str = 'TEST_warc_by_index-001.warc.gz',
):
    # test cli and check output
    index_path = fixture_path / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'
    metadata_record_path = TEST_DATA_PATH / 'filter_cdx/whitelist_10_urls.txt'

    base_prefix = str(base_prefix)

    if extra_args is None:
        extra_args = []

    main(
        args=[
            '-v',
            '--limit=10',
            'repackage',
            f'--cdx-path={str(index_path)}',
            '--write-paths-as-metadata-records',
            str(metadata_record_path),
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
    info_record_headers = None
    response_records = []
    response_contents = []

    metadata_record = None
    metadata_record_headers = None
    metadata_record_content = None

    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                info_record = record.content_stream().read().decode('utf-8')
                info_record_headers = record.rec_headers

            if record.rec_type == 'response':
                response_records.append(record)
                response_contents.append(record.content_stream().read().decode('utf-8', errors='ignore'))

            if record.rec_type == 'metadata':
                metadata_record = record
                metadata_record_content = record.content_stream().read().decode('utf-8')
                metadata_record_headers = record.rec_headers

    assert len(response_records) == 10, 'Invalid record count'

    assert info_record_headers.get('WARC-Filename') == warc_filename

    assert info_record is not None, 'Invalid info record'
    assert 'operator: bob' in info_record, 'Invalid info record'

    assert 'Catalogue en ligne Mission de France' in response_contents[0], 'Invalid response content'
    assert 'dojo/dijit/themes/tundra/tundra' in response_contents[9], 'Invalid response content'

    assert metadata_record is not None, 'Metadata record not set'
    assert metadata_record_content[:10] == 'example.co', 'Invalid metdata record'

    assert metadata_record_headers.get('WARC-Payload-Digest') == 'sha1:VXA2A5YUS3TAY36AUO6MACRMNOH5RXG2', (
        'Invalid metadata block digest'
    )

    # Disabled due to OS-specific line endings
    # assert resource_record_content[-20:-1] == 'hr.fr/produit/t-837', 'Invalid resource record'

    # Calculate expected length based on the actual source file on current OS
    with open(metadata_record_path, 'rb') as f:
        expected_length = len(f.read())

    assert metadata_record.length == expected_length, (
        f'Invalid metadata record length {metadata_record.length}, expected {expected_length} '
        f'(computed from {metadata_record_path} on current OS)'
    )


def test_cli_warc_by_cdx_over_http(tmpdir, caplog):
    assert_cli_warc_by_cdx('https://data.commoncrawl.org', base_prefix=tmpdir, caplog=caplog)


def test_cli_warc_by_cdx_over_http_in_parallel(tmpdir, caplog):
    assert_cli_warc_by_cdx(
        'https://data.commoncrawl.org', base_prefix=tmpdir, caplog=caplog, extra_args=['--parallel=3']
    )


def _produce_range_jobs_csv(tmpdir, csv_name, self_contained=False):
    """Run `repackage` over the CDX fixture with --no-fetch to materialize a range-jobs CSV."""
    import csv as _csv

    index_path = fixture_path / 'filtered_CC-MAIN-2024-30_cdx-00187.gz'
    csv_path = os.path.join(str(tmpdir), csv_name)

    args = [
        '--limit=10',
        'repackage',
        '--target-source=cdx',
        f'--cdx-path={str(index_path)}',
        f'--range-jobs-output={csv_path}',
        '--no-fetch',
    ]
    if self_contained:
        args.append('--csv-self-contained')
    main(args=args)

    with open(csv_path, newline='') as f:
        rows = list(_csv.DictReader(f))
    return csv_path, rows


def _assert_repackaged_warc(warc_path, metadata_record_path):
    """Inspect a repackaged WARC and assert the expected fixture content."""
    response_records = []
    response_contents = []
    metadata_record = None
    metadata_record_headers = None

    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                response_records.append(record)
                response_contents.append(record.content_stream().read().decode('utf-8', errors='ignore'))
            if record.rec_type == 'metadata':
                metadata_record = record
                metadata_record_headers = record.rec_headers

    assert len(response_records) == 10, 'Invalid record count'
    # CsvSource sorts by (warc_filename, offset) by default, so assert content
    # presence independent of record order.
    assert any('Catalogue en ligne Mission de France' in c for c in response_contents), 'Invalid response content'
    assert any('dojo/dijit/themes/tundra/tundra' in c for c in response_contents), 'Invalid response content'
    assert metadata_record is not None, 'Metadata record not set'
    assert metadata_record_headers.get('WARC-Payload-Digest') == 'sha1:VXA2A5YUS3TAY36AUO6MACRMNOH5RXG2', (
        'Invalid metadata block digest'
    )


def test_repackage_csv_materialize_filename(tmpdir):
    """--no-fetch produces a filename-based range-jobs CSV without fetching WARCs."""
    csv_path, rows = _produce_range_jobs_csv(tmpdir, 'ranges.csv')
    assert set(rows[0].keys()) == {'warc_filename', 'warc_record_offset', 'warc_record_length'}
    assert len(rows) == 10
    # No WARC was written for the default --prefix
    assert not any(name.endswith('.warc.gz') for name in os.listdir(str(tmpdir)))


def test_repackage_csv_materialize_self_contained(tmpdir):
    """--csv-self-contained produces a url-based range-jobs CSV."""
    csv_path, rows = _produce_range_jobs_csv(tmpdir, 'ranges_url.csv', self_contained=True)
    assert set(rows[0].keys()) == {'warc_url', 'warc_record_offset', 'warc_record_length'}
    assert len(rows) == 10
    assert rows[0]['warc_url'].startswith('https://data.commoncrawl.org/')


def test_cli_repackage_csv_roundtrip(tmpdir):
    """End-to-end: produce a filename-based ranges CSV, then consume it and fetch over HTTP."""
    metadata_record_path = TEST_DATA_PATH / 'filter_cdx/whitelist_10_urls.txt'
    csv_path, rows = _produce_range_jobs_csv(tmpdir, 'ranges.csv')

    base_prefix = str(tmpdir)
    main(
        args=[
            '-v',
            'repackage',
            '--target-source=csv',
            f'--csv-path={csv_path}',
            '--write-paths-as-metadata-records',
            str(metadata_record_path),
            f'--prefix={base_prefix}/TEST_warc_by_index',
            '--creator=foo',
            '--operator=bob',
            '--warc-download-prefix=https://data.commoncrawl.org',
        ]
    )

    warc_path = os.path.join(base_prefix, 'TEST_warc_by_index-001.warc.gz')
    _assert_repackaged_warc(warc_path, metadata_record_path)


def test_cli_repackage_csv_roundtrip_self_contained(tmpdir):
    """End-to-end with self-contained URLs: header auto-detected on read; no prefix needed."""
    metadata_record_path = TEST_DATA_PATH / 'filter_cdx/whitelist_10_urls.txt'
    csv_path, rows = _produce_range_jobs_csv(tmpdir, 'ranges_url.csv', self_contained=True)

    base_prefix = str(tmpdir)
    main(
        args=[
            '-v',
            'repackage',
            '--target-source=csv',
            f'--csv-path={csv_path}',
            '--write-paths-as-metadata-records',
            str(metadata_record_path),
            f'--prefix={base_prefix}/TEST_warc_by_index',
            '--creator=foo',
            '--operator=bob',
        ]
    )

    warc_path = os.path.join(base_prefix, 'TEST_warc_by_index-001.warc.gz')
    _assert_repackaged_warc(warc_path, metadata_record_path)


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
                'repackage',
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
            'repackage',
            f'--cdx-path={str(index_path)}',
            f'--prefix={str(tmpdir)}/TEST',
            '--subprefix=SUB',
            '--creator=test_creator',
            '--operator=test_operator',
        ]
    )

    # Check that WARC file was created with subprefix
    warc_path = os.path.join(tmpdir, 'TEST-SUB-001.warc.gz')
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
            'repackage',
            f'--cdx-path={str(index_path)}',
            f'--prefix={str(tmpdir)}/TEST_NO_META',
        ]
    )

    # Check that WARC file was created
    warc_path = os.path.join(tmpdir, 'TEST_NO_META-001.warc.gz')
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


@requires_aws_athena
def test_cli_warc_by_athena(
    tmpdir,
    caplog,
):
    base_prefix = tmpdir
    warc_download_prefix = 's3://commoncrawl'
    extra_args: Optional[List[str]] = None
    warc_filename: str = 'TEST_warc_by_index-001.warc.gz'
    base_prefix = str(base_prefix)

    if extra_args is None:
        extra_args = []

    main(
        args=[
            '-v',
            '--limit=10',
            'repackage',
            '--target-source=sql',
            '--engine=athena',
            '--athena-database=ccindex',
            '--athena-s3-output=s3://commoncrawl-ci-temp/athena-results/',
            '--hostnames',
            'oceancolor.sci.gsfc.nasa.gov',
            'example.com',
            '--confirm-cost',
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
