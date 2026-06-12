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

    if target == 'cdx':
        from cdx_toolkit.filter_warc.sources.cdx import CdxSource
        cdx_paths = get_cdx_paths(args.cdx_path, args.cdx_glob)
        return CdxSource(cdx_paths, warc_download_prefix)

    if target == 'csv':
        from cdx_toolkit.filter_warc.sources.csv import CsvSource
        if not args.csv_path:
            raise ValueError('--csv-path is required for --target-source csv')
        return CsvSource(args.csv_path, warc_download_prefix)

    if target == 'sql':
        return _make_sql_source(args, warc_download_prefix, record_limit)

    raise ValueError(f'Invalid target source: {target} (available: cdx, sql, csv)')


def _resolve_sql_query_spec(args) -> Tuple[Optional[str], Optional[list], Optional[list]]:
    """Validate the query-defining flags and return (raw_sql, hostnames, crawls).

    Exactly one of {--hostnames} / {--query|--query-file} must be given. For the
    guided (hostnames) path, --crawl is resolved to concrete crawl names."""
    raw_sql = args.query
    if args.query_file:
        if raw_sql:
            raise ValueError('--query and --query-file are mutually exclusive')
        with open(args.query_file) as f:
            raw_sql = f.read()

    if raw_sql and args.hostnames:
        raise ValueError('--query/--query-file are mutually exclusive with --hostnames')
    if not raw_sql and not args.hostnames:
        raise ValueError('the sql target requires either --hostnames or --query/--query-file')

    if raw_sql:
        return raw_sql, None, None

    crawls = resolve_crawl_names(args.crawl) if args.crawl else None
    return None, args.hostnames, crawls


def _make_sql_source(args, warc_download_prefix, record_limit) -> RangeJobSource:
    engine = args.engine
    if not engine:
        raise ValueError('--engine is required for --target-source sql (choices: athena, duckdb)')

    raw_sql, hostnames, crawls = _resolve_sql_query_spec(args)
    limit = 0 if record_limit is None else record_limit

    if engine == 'athena':
        from cdx_toolkit.filter_warc.sources.athena import AthenaSource
        if not args.athena_s3_output:
            raise ValueError('--athena-s3-output is required for --engine athena')
        database = args.athena_database or 'ccindex'
        query = raw_sql if raw_sql else build_athena_query(hostnames, crawls=crawls, limit=limit)
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
            crawls=crawls,
            index_path=args.duckdb_index_path,
            warc_download_prefix=warc_download_prefix,
            limit=limit,
        )

    raise ValueError(f'Invalid --engine: {engine} (choices: athena, duckdb)')
