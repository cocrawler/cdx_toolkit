
import asyncio
import logging
import time
from typing import Iterable

from cdx_toolkit.filter_warc.aioboto3_utils import RangeJob, parse_s3_uri
from cdx_toolkit.filter_warc.aioboto3_warc_filter import _STOP


logger = logging.getLogger(__name__)


async def get_range_jobs_from_athena(
    client,
    database: str,
    s3_output_location: str,
    key_queue: asyncio.Queue,
    url_host_names: list[str],
    warc_download_prefix: str,
    num_fetchers: int,
    limit: int = 0,
    max_wait_time: int = 300,
):
    """Generate range job based on Athena query -> RangeJob (WARC files and offets) -> key_queue."""

    logger.info('Range index limit: %i', limit)
    count = 0

    # Build query
    tlds = set([url.split(".")[-1] for url in url_host_names])  # unique TLDs
    query_tlds = " OR ".join([f" url_host_tld = '{tld}'" for tld in tlds])
    query_hostnames = " OR ".join([f" url_host_name = '{host_name}'" for host_name in url_host_names])
    query_limit = f"LIMIT {limit}" if limit > 0 else ""

    query = f"""
    SELECT
        warc_filename, warc_record_offset, warc_record_length
    FROM ccindex
    WHERE subset = 'warc'
        AND ({query_tlds}) -- help the query optimizer
        AND ({query_hostnames})
    {query_limit}"""

    logger.info("Executing Athena query...")

    # Start query execution
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": s3_output_location},
    )

    query_execution_id = response["QueryExecutionId"]

    logger.info(f"Query execution started. ID: {query_execution_id}")
    status = _wait_for_query_completion(client, query_execution_id, max_wait_time)

    if status == "SUCCEEDED":
        for range_job in _get_query_results(client, query_execution_id, warc_download_prefix):
            await key_queue.put(range_job)
            count += 1
    else:
        raise Exception(f"Query failed with status: {status}")

    # signal fetchers to stop
    for _ in range(num_fetchers):
        await key_queue.put(_STOP)

    logger.info('Athena query enqueued %d jobs', count)


def _wait_for_query_completion(client, query_execution_id: str, max_wait_time: int) -> str:
    """Wait for query to complete and return final status"""
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        response = client.get_query_execution(QueryExecutionId=query_execution_id)

        status = response["QueryExecution"]["Status"]["State"]
        logger.info(f"Query status: {status}")

        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            if status == "FAILED":
                error_reason = response["QueryExecution"]["Status"].get("StateChangeReason", "Unknown error")
                logger.info(f"Query failed: {error_reason}")
            return status

        time.sleep(2)

    raise TimeoutError(f"Query did not complete within {max_wait_time} seconds")


def _get_query_results(client, query_execution_id: str, warc_download_prefix: str) -> Iterable[RangeJob]:
    """Retrieve query results and convert to pandas DataFrame"""
    # Get query results
    paginator = client.get_paginator("get_query_results")
    page_iterator = paginator.paginate(QueryExecutionId=query_execution_id)
    column_names = None

    for page in page_iterator:
        rows = page["ResultSet"]["Rows"]

        # Get column names from first page
        if column_names is None and rows:
            column_names = [col["VarCharValue"] for col in rows[0]["Data"]]
            rows = rows[1:]  # Skip header row

        # Process data rows
        for row in rows:
            row_data = []
            for cell in row["Data"]:
                value = cell.get("VarCharValue", None)
                row_data.append(value)
            
            row = dict(zip(column_names, row_data))

            warc_url = warc_download_prefix + row["warc_filename"]
            bucket, key = parse_s3_uri(warc_url)

            yield RangeJob(bucket=bucket, key=key, offset=row["warc_record_offset"], length=row["warc_record_length"])


def get_databases(client) -> list:
    """Get list of available databases"""
    response = client.list_databases(CatalogName="AwsDataCatalog")
    return [db["Name"] for db in response["DatabaseList"]]
