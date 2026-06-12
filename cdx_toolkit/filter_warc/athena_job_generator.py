import asyncio
import logging
import re
import time
from typing import Any, Iterable, List, Optional

from cdx_toolkit.filter_warc.data_classes import RangeJob


logger = logging.getLogger(__name__)

# Required output columns that any Athena query (built or raw) must provide.
REQUIRED_RESULT_COLUMNS = ('warc_filename', 'warc_record_offset', 'warc_record_length')

# Athena pricing is ~$5 per TB scanned (used for the post-run cost estimate).
ATHENA_USD_PER_TB = 5.0

# Hostnames, TLDs and crawl names only ever contain these characters. Validating
# against this set both prevents SQL injection and catches malformed input early.
_SQL_LITERAL_RE = re.compile(r'^[A-Za-z0-9.\-]+$')


def escape_sql_literal(value: str) -> str:
    """Validate and quote a value for safe inclusion in an Athena SQL string literal.

    Only hostname/TLD/crawl-name characters (letters, digits, dot, hyphen) are
    allowed, so the result cannot break out of the quotes or inject SQL."""
    if not isinstance(value, str) or not _SQL_LITERAL_RE.match(value):
        raise ValueError(
            f'Invalid value for Athena query literal: {value!r} '
            '(allowed characters: letters, digits, dot, hyphen)'
        )
    return "'" + value + "'"


def build_athena_query(
    url_host_names: List[str],
    crawls: Optional[List[str]] = None,
    limit: int = 0,
    table: str = 'ccindex',
) -> str:
    """Build the Athena SQL returning warc_filename/offset/length for the hostnames.

    CommonCrawl provides an index via AWS Athena that we can use to find the file
    names, offsets, and byte lengths for WARC filtering. See
    https://commoncrawl.org/blog/index-to-warc-files-and-urls-in-columnar-format

    If `crawls` is a non-empty list of crawl names (e.g. ['CC-MAIN-2025-33']), a
    `crawl IN (...)` partition filter is added -- this is the main lever for
    reducing Athena scan cost."""
    if not url_host_names:
        raise ValueError('build_athena_query requires at least one hostname')

    tlds = sorted({url.split('.')[-1] for url in url_host_names})
    query_tlds = ' OR '.join(f'url_host_tld = {escape_sql_literal(tld)}' for tld in tlds)
    query_hostnames = ' OR '.join(f'url_host_name = {escape_sql_literal(h)}' for h in url_host_names)

    where_clauses = [
        "subset = 'warc'",
        f'({query_tlds}) -- help the query optimizer',
        f'({query_hostnames})',
    ]
    # TODO wire --from/--to into a fetch_time BETWEEN ... clause here
    if crawls:
        crawl_in = ', '.join(escape_sql_literal(c) for c in crawls)
        where_clauses.append(f'crawl IN ({crawl_in})')

    where_sql = '\n        AND '.join(where_clauses)
    query_limit = f'\n    LIMIT {limit}' if limit > 0 else ''

    return f"""
    SELECT
        warc_filename, warc_record_offset, warc_record_length
    FROM {table}
    WHERE {where_sql}{query_limit}"""


def validate_result_columns(column_names) -> None:
    """Raise a clear error if the query result lacks the required columns."""
    missing = [c for c in REQUIRED_RESULT_COLUMNS if c not in (column_names or [])]
    if missing:
        raise ValueError(
            'Athena query result is missing required columns: ' + ', '.join(missing) +
            '. The query (including a raw --athena-query) must SELECT ' +
            ', '.join(REQUIRED_RESULT_COLUMNS) + '.'
        )


def join_warc_url(prefix: Optional[str], warc_filename: str) -> str:
    """Join a download prefix with a warc_filename robustly.

    - if warc_filename is already absolute (contains '://'), return it unchanged
      (supports custom queries whose warc_filename is a full s3://-/https:// URL);
    - if prefix is empty/None, return warc_filename unchanged;
    - otherwise join with exactly one '/' (no double slash, no missing slash)."""
    if '://' in warc_filename:
        return warc_filename
    if not prefix:
        return warc_filename
    return prefix.rstrip('/') + '/' + warc_filename.lstrip('/')


def run_athena_query(client, query: str, database: str, s3_output_location: str, max_wait_time: int = 300) -> str:
    """Start an Athena query and block until it completes; return the execution id.

    Raises if the query does not reach the SUCCEEDED state. If the wait is
    interrupted (Ctrl-C / cancellation) or times out, the query is cancelled
    server-side so we don't keep paying for a scan whose results we'll never read."""
    logger.info('Executing Athena query: %s', query)

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': s3_output_location},
    )

    query_execution_id = response['QueryExecutionId']
    logger.info('Query execution started. ID: %s', query_execution_id)

    try:
        status = _wait_for_query_completion(client, query_execution_id, max_wait_time)
    except BaseException:
        # Ctrl-C, asyncio cancellation, or timeout: stop the query server-side
        # to bound the Athena scan cost, then propagate the original exception.
        _stop_query(client, query_execution_id)
        raise

    if status != 'SUCCEEDED':
        raise Exception(f'Query failed with status: {status}')

    return query_execution_id


def _stop_query(client, query_execution_id: str) -> None:
    """Best-effort cancellation of a running Athena query."""
    logger.warning('Cancelling Athena query %s ...', query_execution_id)
    try:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        logger.warning('Athena query %s cancelled', query_execution_id)
    except Exception as e:  # pragma: no cover - best-effort cleanup
        logger.warning('Failed to cancel Athena query %s: %r', query_execution_id, e)


def report_query_cost(client, query_execution_id: str) -> None:
    """Log the bytes scanned and an estimated USD cost for a completed query."""
    try:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)
        scanned = response['QueryExecution'].get('Statistics', {}).get('DataScannedInBytes')
    except Exception as e:  # pragma: no cover - best-effort reporting
        logger.debug('unable to read Athena query statistics: %r', e)
        return

    if scanned is None:
        return

    gb = scanned / 1e9
    usd = scanned / 1e12 * ATHENA_USD_PER_TB
    logger.info('Athena scanned %.2f GB, estimated cost ~$%.4f', gb, usd)


async def get_range_jobs_from_athena(
    client,
    query: str,
    database: str,
    s3_output_location: str,
    job_queue: asyncio.Queue,
    queue_stop_object: Any,
    warc_download_prefix: str,
    num_fetchers: int,
    max_wait_time: int = 300,
) -> int:
    """Execute a prepared Athena query and enqueue a RangeJob per result row.

    The query string is built and validated by the caller (see build_athena_query
    and cdx_toolkit.filter_warc.command). This function only executes it, maps the
    results to RangeJob objects, pushes them to the asyncio queue, signals the
    fetchers to stop, and logs the scan cost."""
    count = 0

    query_execution_id = run_athena_query(client, query, database, s3_output_location, max_wait_time)

    for range_job in iter_range_jobs(client, query_execution_id, warc_download_prefix):
        await job_queue.put(range_job)
        count += 1

    report_query_cost(client, query_execution_id)

    # Signal fetchers to stop
    for _ in range(num_fetchers):
        await job_queue.put(queue_stop_object)

    logger.info('Athena query enqueued %d jobs', count)

    return count


def _wait_for_query_completion(client, query_execution_id: str, max_wait_time: int) -> str:
    """Wait for query to complete and return final status"""
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)

        status = response['QueryExecution']['Status']['State']
        logger.info(f'Query status: {status}')

        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            if status == 'FAILED':
                error_reason = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.info(f'Query failed: {error_reason}')
            return status

        time.sleep(2)

    raise TimeoutError(f'Query did not complete within {max_wait_time} seconds')


def iter_range_jobs(client, query_execution_id: str, warc_download_prefix: str) -> Iterable[RangeJob]:
    """Retrieve query results and convert each row to a RangeJob"""
    # Get query results
    paginator = client.get_paginator('get_query_results')
    page_iterator = paginator.paginate(QueryExecutionId=query_execution_id)
    column_names = None

    for page in page_iterator:
        rows = page['ResultSet']['Rows']

        # Get column names from first page
        if column_names is None and rows:
            column_names = [col['VarCharValue'] for col in rows[0]['Data']]
            validate_result_columns(column_names)
            rows = rows[1:]  # Skip header row

        # Process data rows
        for row in rows:
            row_data = []
            for cell in row['Data']:
                value = cell.get('VarCharValue', None)
                row_data.append(value)

            row = dict(zip(column_names, row_data))

            warc_url = join_warc_url(warc_download_prefix, row['warc_filename'])

            yield RangeJob(url=warc_url, offset=int(row['warc_record_offset']), length=int(row['warc_record_length']))


def get_databases(client) -> list:
    """Get list of available databases"""
    response = client.list_databases(CatalogName='AwsDataCatalog')
    return [db['Name'] for db in response['DatabaseList']]
