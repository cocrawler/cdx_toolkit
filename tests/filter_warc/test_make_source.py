from argparse import Namespace
from unittest.mock import patch

import pytest

from cdx_toolkit.filter_warc.sources import make_source
from cdx_toolkit.filter_warc.sources.athena import AthenaSource
from cdx_toolkit.filter_warc.sources.cdx import CdxSource
from cdx_toolkit.filter_warc.sources.csv import CsvSource
from cdx_toolkit.filter_warc.sources.duckdb import DuckDbSource


def make_args(**kw):
    defaults = dict(
        target_source='cdx',
        engine=None,
        hostnames=None,
        domains=None,
        query=None,
        query_file=None,
        athena_database=None,
        athena_s3_output='s3://commoncrawl-ci-temp/athena-results/',
        duckdb_index_path='s3://commoncrawl/cc-index/table/cc-main/warc/',
        csv_path=None,
        cdx_path=None,
        cdx_glob=None,
        crawl=None,
        no_sort_ranges=False,
    )
    defaults.update(kw)
    return Namespace(**defaults)


def build(**kw):
    return make_source(make_args(**kw), warc_download_prefix='https://data.commoncrawl.org', record_limit=0)


# --- cdx / csv ---

def test_cdx_source():
    src = build(target_source='cdx', cdx_path='/tmp/index.cdx.gz')
    assert isinstance(src, CdxSource)
    assert src.estimate_cost() is None


def test_csv_requires_path():
    with pytest.raises(ValueError):
        build(target_source='csv')


def test_csv_source():
    src = build(target_source='csv', csv_path='/tmp/ranges.csv')
    assert isinstance(src, CsvSource)
    assert src.estimate_cost() is None


# --- sql: engine + query validation ---

def test_sql_requires_engine():
    with pytest.raises(ValueError):
        build(target_source='sql', hostnames=['example.com'])


def test_sql_hostnames_and_query_mutually_exclusive():
    with pytest.raises(ValueError):
        build(target_source='sql', engine='athena', hostnames=['example.com'], query='SELECT 1')


def test_sql_query_and_query_file_mutually_exclusive(tmp_path):
    f = tmp_path / 'q.sql'
    f.write_text('SELECT 1')
    with pytest.raises(ValueError):
        build(target_source='sql', engine='athena', query='SELECT 1', query_file=str(f))


def test_sql_neither_hostnames_domains_nor_query():
    with pytest.raises(ValueError):
        build(target_source='sql', engine='athena')


def test_sql_domains_and_query_mutually_exclusive():
    with pytest.raises(ValueError):
        build(target_source='sql', engine='athena', domains=['example.com'], query='SELECT 1')


def test_athena_domains_only():
    src = build(target_source='sql', engine='athena', domains=['example.com'])
    assert isinstance(src, AthenaSource)
    assert 'url_host_registered_domain = \'example.com\'' in src.query
    assert 'url_host_name' not in src.query


def test_athena_hostnames_and_domains_combined():
    src = build(target_source='sql', engine='athena', hostnames=['www.example.com'], domains=['example.org'])
    assert "url_host_name = 'www.example.com'" in src.query
    assert "url_host_registered_domain = 'example.org'" in src.query
    # TLDs from both hostnames and domains
    assert "url_host_tld = 'com'" in src.query
    assert "url_host_tld = 'org'" in src.query


def test_duckdb_domains_only():
    src = build(target_source='sql', engine='duckdb', domains=['commoncrawl.org'])
    q = src._build_query()
    assert "url_host_registered_domain = 'commoncrawl.org'" in q
    assert 'read_parquet' in q


# --- athena ---

def test_athena_requires_s3_output():
    with pytest.raises(ValueError):
        build(target_source='sql', engine='athena', hostnames=['example.com'], athena_s3_output=None)


def test_athena_built_no_crawl_unbounded():
    src = build(target_source='sql', engine='athena', hostnames=['example.com'])
    assert isinstance(src, AthenaSource)
    est = src.estimate_cost()
    assert est.engine == 'athena' and est.n_crawls is None
    assert 'ccindex' in src.query and 'example.com' in src.query


def test_athena_raw_query_unbounded():
    src = build(target_source='sql', engine='athena', query='SELECT warc_filename FROM x')
    assert src.query == 'SELECT warc_filename FROM x'
    assert src.estimate_cost().n_crawls is None


def test_athena_with_crawls_counts():
    with patch(
        'cdx_toolkit.filter_warc.sources.factory.resolve_crawl_names',
        return_value=['CC-MAIN-2025-33', 'CC-MAIN-2025-30'],
    ):
        src = build(
            target_source='sql', engine='athena', hostnames=['example.com'],
            crawl='CC-MAIN-2025-33,CC-MAIN-2025-30',
        )
    assert src.estimate_cost().n_crawls == 2
    assert 'crawl IN' in src.query


# --- duckdb ---

def test_duckdb_built_query_has_read_parquet_and_partition():
    with patch(
        'cdx_toolkit.filter_warc.sources.factory.resolve_crawl_names',
        return_value=['CC-MAIN-2026-17'],
    ):
        src = build(target_source='sql', engine='duckdb', hostnames=['commoncrawl.org'], crawl='CC-MAIN-2026-17')
    assert isinstance(src, DuckDbSource)
    assert src.estimate_cost().n_crawls == 1
    query = src._build_query()
    assert 'read_parquet' in query
    assert 'crawl=CC-MAIN-2026-17' in query
    assert 'commoncrawl.org' in query


def test_duckdb_no_crawl_unbounded():
    src = build(target_source='sql', engine='duckdb', hostnames=['commoncrawl.org'])
    assert src.estimate_cost().n_crawls is None
    assert 'crawl=*' in src._build_query()


# --- sort by (warc_filename, warc_record_offset) ---

def test_athena_built_query_orders_by_default():
    src = build(target_source='sql', engine='athena', hostnames=['example.com'])
    assert 'ORDER BY warc_filename, warc_record_offset' in src.query


def test_athena_built_query_no_sort():
    src = build(target_source='sql', engine='athena', hostnames=['example.com'], no_sort_ranges=True)
    assert 'ORDER BY' not in src.query


def test_athena_raw_query_not_reordered():
    # a raw query is the user's responsibility; we must not inject ORDER BY
    src = build(target_source='sql', engine='athena', query='SELECT warc_filename FROM x')
    assert 'ORDER BY' not in src.query


def test_duckdb_built_query_orders_by_default():
    src = build(target_source='sql', engine='duckdb', hostnames=['commoncrawl.org'])
    assert 'ORDER BY warc_filename, warc_record_offset' in src._build_query()


def test_duckdb_built_query_no_sort():
    src = build(target_source='sql', engine='duckdb', hostnames=['commoncrawl.org'], no_sort_ranges=True)
    assert 'ORDER BY' not in src._build_query()


def test_csv_source_sort_flag():
    src = build(target_source='csv', csv_path='/tmp/ranges.csv')
    assert src.sort is True
    src = build(target_source='csv', csv_path='/tmp/ranges.csv', no_sort_ranges=True)
    assert src.sort is False
