"""Gated end-to-end tests for the SQL sources (Athena, DuckDB).

These query only a single crawl partition for a single host (cheap, partition-pruned
-- never an all-crawls scan), materialize a range-jobs CSV with --no-fetch, then
consume it and fetch the WARCs over HTTP. They require AWS credentials (and duckdb)
and are skipped in CI.
"""
import csv
import os

import fsspec
from warcio.archiveiterator import ArchiveIterator

from cdx_toolkit.cli import main
from tests.conftest import requires_aws_athena, requires_aws_s3, requires_duckdb, TEST_ATHENA_S3_LOCATION


CRAWL = 'CC-MAIN-2026-17'
HOST = 'commoncrawl.org'


def _produce_and_consume(tmpdir, produce_args):
    csv_path = os.path.join(str(tmpdir), 'ranges.csv')

    # Produce: run the (cheap, single-crawl) SQL query, write range jobs, no WARC fetch.
    main(args=produce_args + [f'--range-jobs-output={csv_path}', '--no-fetch'])

    with open(csv_path, newline='') as f:
        rows = list(csv.DictReader(f))
    assert len(rows) > 0, 'expected at least one successful warc fetch for the host/crawl'

    # Consume: read the CSV, fetch the WARC records over HTTP, write a new WARC.
    base_prefix = str(tmpdir)
    main(
        args=[
            'repackage',
            '--target-source=csv',
            f'--csv-path={csv_path}',
            f'--prefix={base_prefix}/TEST_sql',
            '--warc-download-prefix=https://data.commoncrawl.org',
        ]
    )

    warc_path = os.path.join(base_prefix, 'TEST_sql-001.warc.gz')
    response_count = 0
    with fsspec.open(warc_path, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                response_count += 1
                target = record.rec_headers.get_header('WARC-Target-URI') or ''
                assert HOST in target, f'unexpected target URI: {target}'

    assert response_count == len(rows), 'every range job should yield a response record'


@requires_aws_athena
def test_repackage_sql_athena_e2e(tmpdir):
    _produce_and_consume(
        tmpdir,
        [
            '--crawl', CRAWL,
            'repackage',
            '--target-source=sql',
            '--engine=athena',
            '--athena-database=ccindex',
            f'--athena-s3-output={TEST_ATHENA_S3_LOCATION}',
            '--hostnames', HOST,
            '--confirm-cost',
        ],
    )


@requires_aws_s3
@requires_duckdb
def test_repackage_sql_duckdb_e2e(tmpdir):
    _produce_and_consume(
        tmpdir,
        [
            '--crawl', CRAWL,
            'repackage',
            '--target-source=sql',
            '--engine=duckdb',
            '--hostnames', HOST,
            '--confirm-cost',
        ],
    )


@requires_aws_s3
@requires_duckdb
def test_repackage_sql_duckdb_extra_columns_e2e(tmpdir):
    # A raw --query that SELECTs an extra analysis column (content_languages) should
    # carry that column through to the materialized range-jobs CSV. Single crawl +
    # LIMIT keeps the scan cheap.
    csv_path = os.path.join(str(tmpdir), 'ranges.csv')
    query = (
        'SELECT warc_filename, warc_record_offset, warc_record_length, content_languages '
        "FROM read_parquet("
        f"'s3://commoncrawl/cc-index/table/cc-main/warc/crawl={CRAWL}/subset=warc/*.parquet', "
        'hive_partitioning=true) '
        f"WHERE url_host_registered_domain = '{HOST}' LIMIT 10"
    )
    main(args=[
        'repackage',
        '--target-source=sql',
        '--engine=duckdb',
        f'--query={query}',
        f'--range-jobs-output={csv_path}',
        '--no-fetch',
        '--confirm-cost',
    ])

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        assert 'content_languages' in (reader.fieldnames or [])
        rows = list(reader)
    assert len(rows) > 0
    assert any(r.get('content_languages') for r in rows), 'expected a content_languages value'


@requires_aws_s3
@requires_duckdb
def test_repackage_sql_duckdb_domain_e2e(tmpdir):
    # Domain filtering (url_host_registered_domain) also matches subdomains; bound it
    # with --limit to keep the live verification cheap.
    _produce_and_consume(
        tmpdir,
        [
            '--crawl', CRAWL,
            '--limit', '10',
            'repackage',
            '--target-source=sql',
            '--engine=duckdb',
            '--domains', HOST,
            '--confirm-cost',
        ],
    )
