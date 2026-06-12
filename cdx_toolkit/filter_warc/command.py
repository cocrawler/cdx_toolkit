from cdx_toolkit.filter_warc.warc_filter import WARCFilter
from cdx_toolkit.filter_warc.sources import make_source
from cdx_toolkit.utils import get_version


import fsspec


import sys
import time
import logging

logger = logging.getLogger(__name__)

# A SQL query restricted to at most this many crawls is considered cheap enough to
# run without a cost-confirmation prompt.
LARGE_CRAWL_SET_THRESHOLD = 10


def confirm_cost(estimate, confirmed) -> None:
    """Prompt before running a potentially expensive index scan.

    `estimate` is a CostEstimate (or None for sources that never bill, e.g. cdx/csv).
    A scan bounded to <= LARGE_CRAWL_SET_THRESHOLD crawls runs without a prompt.
    Otherwise (no crawl filter, a large crawl set, or unverifiable raw SQL) we confirm
    interactively, abort in non-interactive sessions, unless `confirmed` (--confirm-cost)."""
    if confirmed or estimate is None:
        return

    n_crawls = estimate.n_crawls
    if n_crawls is not None and n_crawls <= LARGE_CRAWL_SET_THRESHOLD:
        return

    engine = estimate.engine
    if n_crawls is None:
        reason = (f'This {engine} query is not restricted to specific crawls (or uses custom SQL '
                  'whose crawl partition pruning could not be verified) and may scan ALL crawls.')
    else:
        reason = (f'This {engine} query is restricted to {n_crawls} crawls (more than '
                  f'{LARGE_CRAWL_SET_THRESHOLD}) and may scan a large amount of data.')

    if not sys.stdin.isatty():
        raise SystemExit(
            reason + ' Refusing to run a potentially expensive scan in non-interactive mode. '
            'Restrict with --crawl (<=10 crawls), or pass --confirm-cost.'
        )

    logger.warning(reason)
    answer = input('Index SQL scans can be expensive (Athena bills per TB scanned). Proceed? [y/N] ')
    if answer.strip().lower() not in ('y', 'yes'):
        raise SystemExit('Aborted by user.')


def run_repackage(args, cmdline):
    """Repackage WARC records from a pluggable range-job source (cdx / sql / csv).

    Approach:
    - Generate RangeJobs (WARC file + byte range) from the selected source.
    - Optionally materialize them to a CSV (--range-jobs-output; --no-fetch to skip fetching).
    - Fetch each WARC record and write a new WARC, including metadata records.
    """
    logger.info('Repackaging WARC files (target source: %s)', args.target_source)

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
        else 'warc extraction generated with: ' + cmdline,
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

    # Build the source (validates source/engine/query options) and confirm scan cost
    # up front (synchronously) before launching the pipeline.
    source = make_source(args, warc_download_prefix=args.warc_download_prefix, record_limit=limit)
    confirm_cost(source.estimate_cost(), args.confirm_cost)

    # make sure the output base dir exists (only needed when actually writing WARCs)
    if not args.no_fetch:
        prefix_fs, prefix_fs_path = fsspec.url_to_fs(prefix_path)
        prefix_fs.makedirs(prefix_fs._parent(prefix_fs_path), exist_ok=True)

    warc_filter = WARCFilter(
        source=source,
        range_jobs_output=args.range_jobs_output,
        no_fetch=args.no_fetch,
        csv_self_contained=args.csv_self_contained,
        prefix_path=prefix_path,
        writer_info=info,
        writer_subprefix=args.subprefix,
        write_paths_as_metadata_records=write_paths_as_metadata_records,
        record_limit=limit,
        log_every_n=log_every_n,
        warc_download_prefix=args.warc_download_prefix,
        n_parallel=n_parallel,
        max_file_size=args.size,
    )
    records_n = warc_filter.filter()

    logger.info('WARC records extracted: %i', records_n)

    # End timing and log execution time
    end_time = time.time()
    execution_time = end_time - start_time

    logger.info(f'Script execution time: {execution_time:.3f} seconds')
