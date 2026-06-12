from cdx_toolkit.filter_warc.cdx_utils import get_cdx_paths
from cdx_toolkit.filter_warc.warc_filter import WARCFilter
from cdx_toolkit.filter_warc.athena_job_generator import build_athena_query
from cdx_toolkit.commoncrawl import normalize_crawl, get_cc_endpoints, match_cc_crawls
from cdx_toolkit.utils import get_version


import fsspec


import sys
import time
import logging

logger = logging.getLogger(__name__)

# A built Athena query restricted to at most this many crawls is considered cheap
# enough to run without a cost-confirmation prompt.
LARGE_CRAWL_SET_THRESHOLD = 10

# Default Common Crawl index mirror used to resolve --crawl to concrete crawl names.
CC_INDEX_MIRROR = 'https://index.commoncrawl.org/'


def _endpoint_to_crawl_name(endpoint: str) -> str:
    """Turn a collinfo cdx-api endpoint into its crawl name.

    e.g. 'https://index.commoncrawl.org/CC-MAIN-2025-33-index' -> 'CC-MAIN-2025-33'."""
    name = endpoint.rstrip('/').split('/')[-1]
    if name.endswith('-index'):
        name = name[:-len('-index')]
    return name


def _resolve_crawl_names(crawl_arg) -> list:
    """Resolve a --crawl value to concrete CC-MAIN crawl names for the partition filter.

    Reuses the CDX path's helpers so `--crawl` accepts the same forms (comma-separated
    names or an integer for the most recent N crawls)."""
    crawls = normalize_crawl([crawl_arg])
    raw_index_list = get_cc_endpoints(CC_INDEX_MIRROR)
    matched = match_cc_crawls(crawls, raw_index_list)
    return [_endpoint_to_crawl_name(ep) for ep in matched]


def resolve_athena_query(args):
    """Validate the Athena args and return (sql, n_crawls).

    n_crawls is the number of crawls the query is restricted to, or None when the
    query is unrestricted (scans all crawls) or its pruning cannot be verified
    (raw --athena-query/--athena-query-file)."""
    raw_sql = args.athena_query
    if args.athena_query_file:
        if raw_sql:
            raise ValueError('--athena-query and --athena-query-file are mutually exclusive')
        with open(args.athena_query_file) as f:
            raw_sql = f.read()

    if raw_sql and args.athena_hostnames:
        raise ValueError('--athena-query/--athena-query-file are mutually exclusive with --athena-hostnames')
    if not raw_sql and not args.athena_hostnames:
        raise ValueError('athena target requires either --athena-hostnames or --athena-query/--athena-query-file')

    if not args.athena_database:
        raise ValueError('--athena-database is required for target source `athena`')
    if not args.athena_s3_output:
        raise ValueError('--athena-s3-output is required for target source `athena`')

    if raw_sql:
        # Crawl-partition pruning of a raw query cannot be verified -> treat as unbounded.
        return raw_sql, None

    # Guided/built path
    limit = 0 if args.limit is None else args.limit
    if args.crawl:
        crawl_names = _resolve_crawl_names(args.crawl)
        sql = build_athena_query(args.athena_hostnames, crawls=crawl_names, limit=limit)
        return sql, len(crawl_names)

    sql = build_athena_query(args.athena_hostnames, limit=limit)
    return sql, None


def confirm_athena_cost(n_crawls, confirmed) -> None:
    """Prompt before running a potentially expensive Athena query.

    A built query restricted to <= LARGE_CRAWL_SET_THRESHOLD crawls runs without a
    prompt. Otherwise (no crawl filter, a large crawl set, or unverifiable raw SQL)
    we confirm interactively, abort in non-interactive sessions, unless `confirmed`
    (--confirm-athena-cost) is set."""
    if confirmed:
        return
    if n_crawls is not None and n_crawls <= LARGE_CRAWL_SET_THRESHOLD:
        return

    if n_crawls is None:
        reason = ('This Athena query is not restricted to specific crawls (or uses custom SQL whose '
                  'crawl partition pruning could not be verified) and may scan ALL crawls.')
    else:
        reason = (f'This Athena query is restricted to {n_crawls} crawls (more than '
                  f'{LARGE_CRAWL_SET_THRESHOLD}) and may scan a large amount of data.')

    if not sys.stdin.isatty():
        raise SystemExit(
            reason + ' Refusing to run a potentially expensive Athena scan in non-interactive mode. '
            'Restrict with --crawl (<=10 crawls), or pass --confirm-athena-cost.'
        )

    logger.warning(reason)
    answer = input('Athena bills per TB scanned. Proceed? [y/N] ')
    if answer.strip().lower() not in ('y', 'yes'):
        raise SystemExit('Aborted by user.')


def run_warcer_by_cdx(args, cmdline):
    """Like warcer but fetches WARC records based on one or more CDX index files.

    The CDX files can be filtered using the `filter_cdx` commands based a given URL/SURT list.

    Approach:
    - Iterate over one or more CDX files to extract capture object (file, offset, length)
    - Fetch WARC record based on capture object
    - Write to new WARC file including metadata records with index.
    - The CDX metadata record is written to the WARC directly before for response records that matches to the CDX.
    """
    logger.info('Filtering WARC files based on CDX')

    # Start timing
    start_time = time.time()

    write_paths_as_metadata_records = args.write_paths_as_metadata_records

    if args.is_part_of:
        ispartof = args.is_part_of
    else:
        ispartof = args.prefix
        if args.subprefix:
            ispartof += '-' + args.subprefix

    info = {
        'software': 'pypi_cdx_toolkit/' + get_version(),
        'isPartOf': ispartof,
        'description': args.description
        if args.description
        else 'warc extraction based on CDX generated with: ' + cmdline,
        'format': 'WARC file version 1.0',
    }
    if args.creator:
        info['creator'] = args.creator
    if args.operator:
        info['operator'] = args.operator

    n_parallel = args.parallel
    log_every_n = args.log_every_n
    limit = 0 if args.limit is None else args.limit
    prefix_path = str(args.prefix)
    prefix_fs, prefix_fs_path = fsspec.url_to_fs(prefix_path)

    # make sure the base dir exists
    prefix_fs.makedirs(prefix_fs._parent(prefix_fs_path), exist_ok=True)

    # target source handling
    athena_query = None
    if args.target_source == 'cdx':
        cdx_paths = get_cdx_paths(
            args.cdx_path,
            args.cdx_glob,
        )
    elif args.target_source == "athena":
        cdx_paths = None
        # Build/validate the Athena query up front (synchronously) so we can warn
        # about expensive (unpartitioned / large) scans before launching the pipeline.
        athena_query, n_crawls = resolve_athena_query(args)
        confirm_athena_cost(n_crawls, args.confirm_athena_cost)
    else:
        raise ValueError(f'Invalid target source specified: {args.target_source} (available: cdx, athena)')

    warc_filter = WARCFilter(
        target_source=args.target_source,
        cdx_paths=cdx_paths,
        athena_database=args.athena_database,
        athena_s3_output_location=args.athena_s3_output,
        athena_query=athena_query,
        prefix_path=prefix_path,
        writer_info=info,
        writer_subprefix=args.subprefix,
        write_paths_as_metadata_records=write_paths_as_metadata_records,
        record_limit=limit,
        log_every_n=log_every_n,
        warc_download_prefix=args.warc_download_prefix,
        n_parallel=n_parallel,
        max_file_size=args.size,
        # writer_kwargs=writer_kwargs,
    )
    records_n = warc_filter.filter()

    logger.info('WARC records extracted: %i', records_n)

    # End timing and log execution time
    end_time = time.time()
    execution_time = end_time - start_time

    logger.info(f'Script execution time: {execution_time:.3f} seconds')
