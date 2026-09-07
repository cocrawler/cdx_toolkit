from argparse import Namespace
from unittest.mock import patch

import pytest

from cdx_toolkit.filter_warc import command
from cdx_toolkit.filter_warc.command import resolve_athena_query


def make_args(**kw):
    defaults = dict(
        athena_query=None,
        athena_query_file=None,
        athena_hostnames=None,
        athena_database='ccindex',
        athena_s3_output='s3://commoncrawl-ci-temp/athena-results/',
        crawl=None,
        limit=None,
    )
    defaults.update(kw)
    return Namespace(**defaults)


def test_query_and_hostnames_mutually_exclusive():
    args = make_args(athena_query='SELECT 1', athena_hostnames=['example.com'])
    with pytest.raises(ValueError):
        resolve_athena_query(args)


def test_query_and_query_file_mutually_exclusive(tmp_path):
    f = tmp_path / 'q.sql'
    f.write_text('SELECT 1')
    args = make_args(athena_query='SELECT 1', athena_query_file=str(f))
    with pytest.raises(ValueError):
        resolve_athena_query(args)


def test_neither_hostnames_nor_query():
    args = make_args()
    with pytest.raises(ValueError):
        resolve_athena_query(args)


def test_missing_database():
    args = make_args(athena_hostnames=['example.com'], athena_database=None)
    with pytest.raises(ValueError):
        resolve_athena_query(args)


def test_missing_s3_output():
    args = make_args(athena_hostnames=['example.com'], athena_s3_output=None)
    with pytest.raises(ValueError):
        resolve_athena_query(args)


def test_raw_query_is_unbounded():
    args = make_args(athena_query='SELECT warc_filename FROM x')
    sql, n_crawls = resolve_athena_query(args)
    assert sql == 'SELECT warc_filename FROM x'
    assert n_crawls is None


def test_query_file_is_read(tmp_path):
    f = tmp_path / 'q.sql'
    f.write_text('SELECT warc_filename, warc_record_offset, warc_record_length FROM x')
    args = make_args(athena_query_file=str(f))
    sql, n_crawls = resolve_athena_query(args)
    assert 'warc_filename' in sql
    assert n_crawls is None


def test_built_no_crawl_is_unbounded():
    args = make_args(athena_hostnames=['example.com'])
    sql, n_crawls = resolve_athena_query(args)
    assert 'example.com' in sql
    assert n_crawls is None


def test_built_with_crawls_counts():
    args = make_args(athena_hostnames=['example.com'], crawl='CC-MAIN-2025-33,CC-MAIN-2025-30')
    with patch.object(command, '_resolve_crawl_names', return_value=['CC-MAIN-2025-33', 'CC-MAIN-2025-30']):
        sql, n_crawls = resolve_athena_query(args)
    assert n_crawls == 2
    assert 'crawl IN' in sql
