import logging
from typing import Optional, Tuple

from cdx_toolkit.filter_warc.cdx_utils import get_cdx_paths
from cdx_toolkit.filter_warc.sources.base import RangeJobSource
from cdx_toolkit.filter_warc.sources.sql_base import build_athena_query, resolve_crawl_names


logger = logging.getLogger(__name__)


def make_source(args, *, warc_download_prefix: Optional[str], record_limit: int) -> RangeJobSource:
    """Build the RangeJobSource selected by --target-source (+ --engine for sql).

    Centralises all source/engine validation (engine required iff sql;
    hostnames/query/query-file mutual exclusivity; required connection options)."""
    target = args.target_source
    # Sort range jobs by (warc_filename, warc_record_offset) for fetch-time read
    # locality unless explicitly disabled. Applies to the guided SQL query (ORDER BY)
    # and to CSV loading; cdx files are out of scope (already SURT-ordered).
    sort = not getattr(args, 'no_sort_ranges', False)

    if target == 'cdx':
        from cdx_toolkit.filter_warc.sources.cdx import CdxSource
        cdx_paths = get_cdx_paths(args.cdx_path, args.cdx_glob)
        return CdxSource(cdx_paths, warc_download_prefix)

    if target == 'csv':
        from cdx_toolkit.filter_warc.sources.csv import CsvSource
        if not args.csv_path:
            raise ValueError('--csv-path is required for --target-source csv')
        return CsvSource(args.csv_path, warc_download_prefix, sort=sort)

    if target == 'sql':
        return _make_sql_source(args, warc_download_prefix, record_limit, sort=sort)

    raise ValueError(f'Invalid target source: {target} (available: cdx, sql, csv)')


def _resolve_sql_query_spec(args) -> Tuple[Optional[str], Optional[list], Optional[list], Optional[list]]:
    """Validate the query-defining flags and return (raw_sql, hostnames, domains, crawls).

    The guided path (--hostnames and/or --domains) and a raw query
    (--query/--query-file) are mutually exclusive. For the guided path, --crawl is
    resolved to concrete crawl names."""
    raw_sql = args.query
    if args.query_file:
        if raw_sql:
            raise ValueError('--query and --query-file are mutually exclusive')
        with open(args.query_file) as f:
            raw_sql = f.read()

    has_guided = bool(args.hostnames) or bool(args.domains)
    if raw_sql and has_guided:
        raise ValueError('--query/--query-file are mutually exclusive with --hostnames/--domains')
    if not raw_sql and not has_guided:
        raise ValueError('the sql target requires --hostnames, --domains, or --query/--query-file')

    if raw_sql:
        return raw_sql, None, None, None

    crawls = resolve_crawl_names(args.crawl) if args.crawl else None
    return None, args.hostnames, args.domains, crawls


def _make_sql_source(args, warc_download_prefix, record_limit, *, sort: bool = True) -> RangeJobSource:
    engine = args.engine
    if not engine:
        raise ValueError('--engine is required for --target-source sql (choices: athena, duckdb)')

    raw_sql, hostnames, domains, crawls = _resolve_sql_query_spec(args)
    limit = 0 if record_limit is None else record_limit

    if engine == 'athena':
        from cdx_toolkit.filter_warc.sources.athena import AthenaSource
        if not args.athena_s3_output:
            raise ValueError('--athena-s3-output is required for --engine athena')
        database = args.athena_database or 'ccindex'
        # A raw --query is the user's responsibility to order; only the guided query
        # gets the ORDER BY (warc_filename, warc_record_offset) for read locality.
        query = raw_sql if raw_sql else build_athena_query(
            hostnames, crawls=crawls, limit=limit, url_host_registered_domains=domains,
            order_by=sort,
        )
        n_crawls = None if raw_sql else (len(crawls) if crawls else None)
        return AthenaSource(
            query=query,
            database=database,
            s3_output_location=args.athena_s3_output,
            warc_download_prefix=warc_download_prefix,
            n_crawls=n_crawls,
        )

    if engine == 'duckdb':
        from cdx_toolkit.filter_warc.sources.duckdb import DuckDbSource
        return DuckDbSource(
            query=raw_sql,
            hostnames=hostnames,
            domains=domains,
            crawls=crawls,
            index_path=args.duckdb_index_path,
            warc_download_prefix=warc_download_prefix,
            limit=limit,
            sort=sort,
        )

    raise ValueError(f'Invalid --engine: {engine} (choices: athena, duckdb)')
