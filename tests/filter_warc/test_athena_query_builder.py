import pytest

from cdx_toolkit.filter_warc.sources.sql_base import (
    build_athena_query,
    escape_sql_literal,
    validate_result_columns,
    join_warc_url,
)
from cdx_toolkit.filter_warc.sources.athena import run_athena_query


class _FakeAthenaClient:
    """Minimal stand-in for a boto3 Athena client (no AWS, no network)."""

    def __init__(self, raise_on_wait=None):
        self._raise_on_wait = raise_on_wait
        self.started = []
        self.stopped = []

    def start_query_execution(self, **kwargs):
        self.started.append(kwargs)
        return {'QueryExecutionId': 'qid-123'}

    def get_query_execution(self, QueryExecutionId):
        if self._raise_on_wait is not None:
            raise self._raise_on_wait
        return {'QueryExecution': {'Status': {'State': 'SUCCEEDED'},
                                   'Statistics': {'DataScannedInBytes': 1000}}}

    def stop_query_execution(self, QueryExecutionId):
        self.stopped.append(QueryExecutionId)


def test_build_query_hostnames():
    q = build_athena_query(['example.com', 'test.org'])
    assert "subset = 'warc'" in q
    assert "url_host_tld = 'com'" in q
    assert "url_host_tld = 'org'" in q
    assert "url_host_name = 'example.com'" in q
    assert "url_host_name = 'test.org'" in q
    assert 'warc_filename, warc_record_offset, warc_record_length' in q
    assert 'crawl IN' not in q
    assert 'LIMIT' not in q


def test_build_query_with_crawls():
    q = build_athena_query(['example.com'], crawls=['CC-MAIN-2025-33', 'CC-MAIN-2025-30'])
    assert "crawl IN ('CC-MAIN-2025-33', 'CC-MAIN-2025-30')" in q


def test_build_query_limit():
    assert 'LIMIT 10' in build_athena_query(['example.com'], limit=10)
    assert 'LIMIT' not in build_athena_query(['example.com'], limit=0)


def test_build_query_requires_hostnames():
    with pytest.raises(ValueError):
        build_athena_query([])


def test_escape_sql_literal_valid():
    assert escape_sql_literal('example.com') == "'example.com'"
    assert escape_sql_literal('CC-MAIN-2025-33') == "'CC-MAIN-2025-33'"


@pytest.mark.parametrize('bad', ["a';DROP TABLE x;--", "a' OR '1'='1", 'a b', "a'", "', '"])
def test_escape_sql_literal_rejects_injection(bad):
    with pytest.raises(ValueError):
        escape_sql_literal(bad)


def test_build_query_rejects_injection_hostname():
    with pytest.raises(ValueError):
        build_athena_query(["example.com'; DROP TABLE ccindex; --"])


def test_validate_result_columns_ok():
    validate_result_columns(['warc_filename', 'warc_record_offset', 'warc_record_length', 'extra'])


def test_validate_result_columns_missing():
    with pytest.raises(ValueError) as e:
        validate_result_columns(['warc_filename'])
    assert 'warc_record_offset' in str(e.value)
    assert 'warc_record_length' in str(e.value)


def test_join_warc_url_relative():
    assert (
        join_warc_url('https://data.commoncrawl.org', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )


def test_join_warc_url_no_double_slash():
    assert (
        join_warc_url('https://data.commoncrawl.org/', 'crawl-data/x.warc.gz')
        == 'https://data.commoncrawl.org/crawl-data/x.warc.gz'
    )
    assert join_warc_url('s3://bucket', '/crawl-data/x.warc.gz') == 's3://bucket/crawl-data/x.warc.gz'


def test_join_warc_url_empty_prefix():
    assert join_warc_url('', 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'
    assert join_warc_url(None, 'crawl-data/x.warc.gz') == 'crawl-data/x.warc.gz'


def test_join_warc_url_absolute_filename_passthrough():
    assert join_warc_url('https://data.commoncrawl.org', 's3://cc/x.warc.gz') == 's3://cc/x.warc.gz'
    assert join_warc_url('', 'https://host/x.warc.gz') == 'https://host/x.warc.gz'


def test_run_athena_query_logs_sql(caplog):
    import logging
    client = _FakeAthenaClient()
    with caplog.at_level(logging.INFO):
        run_athena_query(client, 'SELECT my_special_column FROM ccindex', 'ccindex', 's3://b/out/')
    assert 'SELECT my_special_column FROM ccindex' in caplog.text


def test_run_athena_query_success_no_cancel():
    client = _FakeAthenaClient()
    qid = run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert qid == 'qid-123'
    assert client.stopped == []


def test_run_athena_query_cancels_on_interrupt():
    client = _FakeAthenaClient(raise_on_wait=KeyboardInterrupt())
    with pytest.raises(KeyboardInterrupt):
        run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert client.stopped == ['qid-123']


def test_run_athena_query_cancels_on_timeout():
    client = _FakeAthenaClient(raise_on_wait=TimeoutError('too slow'))
    with pytest.raises(TimeoutError):
        run_athena_query(client, 'SELECT 1', 'ccindex', 's3://b/out/')
    assert client.stopped == ['qid-123']
