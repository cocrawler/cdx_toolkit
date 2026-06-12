import logging
import re
from typing import List, Optional

from cdx_toolkit.commoncrawl import normalize_crawl, get_cc_endpoints, match_cc_crawls


logger = logging.getLogger(__name__)

# Required output columns that any index query (built or raw) must provide.
REQUIRED_RESULT_COLUMNS = ('warc_filename', 'warc_record_offset', 'warc_record_length')

# Hostnames, TLDs and crawl names only ever contain these characters. Validating
# against this set both prevents SQL injection and catches malformed input early.
_SQL_LITERAL_RE = re.compile(r'^[A-Za-z0-9.\-]+$')

# Default Common Crawl index mirror used to resolve --crawl to concrete crawl names.
CC_INDEX_MIRROR = 'https://index.commoncrawl.org/'


def escape_sql_literal(value: str) -> str:
    """Validate and quote a value for safe inclusion in a SQL string literal.

    Only hostname/TLD/crawl-name characters (letters, digits, dot, hyphen) are
    allowed, so the result cannot break out of the quotes or inject SQL."""
    if not isinstance(value, str) or not _SQL_LITERAL_RE.match(value):
        raise ValueError(
            f'Invalid value for SQL query literal: {value!r} '
            '(allowed characters: letters, digits, dot, hyphen)'
        )
    return "'" + value + "'"


def build_where_sql(url_host_names: List[str], crawls: Optional[List[str]] = None) -> str:
    """Build the WHERE body (without the `WHERE` keyword) shared by all SQL engines.

    If `crawls` is a non-empty list of crawl names (e.g. ['CC-MAIN-2025-33']), a
    `crawl IN (...)` partition filter is added -- the main lever for reducing scan
    cost. Engines differ only in their FROM clause (see build_sql)."""
    if not url_host_names:
        raise ValueError('an index query requires at least one hostname')

    tlds = sorted({h.split('.')[-1] for h in url_host_names})
    query_tlds = ' OR '.join(f'url_host_tld = {escape_sql_literal(t)}' for t in tlds)
    query_hosts = ' OR '.join(f'url_host_name = {escape_sql_literal(h)}' for h in url_host_names)

    clauses = [
        "subset = 'warc'",
        f'({query_tlds}) -- help the query optimizer',
        f'({query_hosts})',
    ]
    # TODO wire --from/--to into a fetch_time BETWEEN ... clause here
    if crawls:
        crawl_in = ', '.join(escape_sql_literal(c) for c in crawls)
        clauses.append(f'crawl IN ({crawl_in})')

    return '\n        AND '.join(clauses)


def build_sql(
    from_clause: str,
    url_host_names: List[str],
    crawls: Optional[List[str]] = None,
    limit: int = 0,
) -> str:
    """Assemble a full SELECT for the columnar index.

    `from_clause` is the text following FROM (e.g. `ccindex` for Athena, or a
    `read_parquet(...)` expression for DuckDB)."""
    where_sql = build_where_sql(url_host_names, crawls)
    limit_sql = f'\n    LIMIT {limit}' if limit and limit > 0 else ''

    return f"""
    SELECT
        warc_filename, warc_record_offset, warc_record_length
    FROM {from_clause}
    WHERE {where_sql}{limit_sql}"""


def build_athena_query(
    url_host_names: List[str],
    crawls: Optional[List[str]] = None,
    limit: int = 0,
    table: str = 'ccindex',
) -> str:
    """Athena flavour of build_sql (FROM <table>). Kept for the athena_job_generator shim."""
    return build_sql(table, url_host_names, crawls=crawls, limit=limit)


def validate_result_columns(column_names) -> None:
    """Raise a clear error if the query result lacks the required columns."""
    missing = [c for c in REQUIRED_RESULT_COLUMNS if c not in (column_names or [])]
    if missing:
        raise ValueError(
            'Index query result is missing required columns: ' + ', '.join(missing) +
            '. The query (including a raw --query) must SELECT ' +
            ', '.join(REQUIRED_RESULT_COLUMNS) + '.'
        )


def join_warc_url(prefix: Optional[str], warc_filename: str) -> str:
    """Join a download prefix with a warc_filename robustly.

    - if warc_filename is already absolute (contains '://'), return it unchanged
      (supports custom queries / self-contained CSVs whose value is a full URL);
    - if prefix is empty/None, return warc_filename unchanged;
    - otherwise join with exactly one '/' (no double slash, no missing slash)."""
    if '://' in warc_filename:
        return warc_filename
    if not prefix:
        return warc_filename
    return prefix.rstrip('/') + '/' + warc_filename.lstrip('/')


def endpoint_to_crawl_name(endpoint: str) -> str:
    """Turn a collinfo cdx-api endpoint into its crawl name.

    e.g. 'https://index.commoncrawl.org/CC-MAIN-2025-33-index' -> 'CC-MAIN-2025-33'."""
    name = endpoint.rstrip('/').split('/')[-1]
    if name.endswith('-index'):
        name = name[:-len('-index')]
    return name


def resolve_crawl_names(crawl_arg) -> List[str]:
    """Resolve a --crawl value to concrete CC-MAIN crawl names for the partition filter.

    Reuses the CDX path's helpers so `--crawl` accepts the same forms (comma-separated
    names or an integer for the most recent N crawls)."""
    crawls = normalize_crawl([crawl_arg])
    raw_index_list = get_cc_endpoints(CC_INDEX_MIRROR)
    matched = match_cc_crawls(crawls, raw_index_list)
    return [endpoint_to_crawl_name(ep) for ep in matched]
