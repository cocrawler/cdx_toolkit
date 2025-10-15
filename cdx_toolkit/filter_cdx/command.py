from cdx_toolkit.filter_cdx.path_utils import validate_resolved_paths

import logging
import fsspec
from url_is_in import SURTMatcher, convert_url_to_surt_with_wildcard


import sys
import time

from cdx_toolkit.filter_cdx.cdx_filter import filter_cdx
from cdx_toolkit.filter_cdx.path_utils import resolve_paths

logger = logging.getLogger(__name__)


def run_filter_cdx(args, cmdline: str):
    """Filter CDX index files based on a given URL or SURT whitelist.

    - If a URL filter is provided, it is converted to a SURT filter.
    - A index entry's SURT must start with one of the SURTs from the whitelist to be considered.
    - All other index entries are discarded.
    - All input/output paths can be local or remote paths (S3, ...) and compressed (*.gz).
    """
    logger.info('Filtering CDX files based on whitelist')

    # Start timing
    start_time = time.time()

    # Resolve input and output paths using glob pattern
    # This should support glob via S3 (e.g., to fetch the indices from s3://commoncrawl/cc-index/collections/* ...)
    input_paths, output_paths = resolve_paths(
        input_base_path=args.input_base_path,
        input_glob=args.input_glob,
        output_base_path=args.output_base_path,
    )
    validate_resolved_paths(output_paths, args.overwrite)

    logger.info(f'Found {len(input_paths)} files matching pattern: {args.input_base_path}/{args.input_glob}')

    # Load URL or SURT prefixes from file (each line is a surt)
    filter_fs, filter_fs_path = fsspec.url_to_fs(args.filter_file)
    logger.info('Loading whitelist from %s', filter_fs_path)

    if not filter_fs.exists(filter_fs_path):  # Check that surts file exists
        logger.error(f'Filter file not found: {filter_fs_path}')
        sys.exit(1)

    with filter_fs.open(filter_fs_path, 'rt') as input_f:
        include_prefixes = [line.strip() for line in input_f.readlines()]

    logger.info(f'Loaded {len(include_prefixes):,} filter entries')

    # Convert URLs to SURTs
    if args.filter_type == 'url':
        include_prefixes = [convert_url_to_surt_with_wildcard(item_url) for item_url in include_prefixes]

    matcher = SURTMatcher(include_prefixes, match_subdomains=True)

    limit = 0 if args.limit is None else args.limit

    # Process files in parallel
    total_lines_n, total_included_n, total_errors_n = filter_cdx(
        matcher=matcher,
        input_paths=input_paths,
        output_paths=output_paths,
        limit=limit,
        n_parallel=max(1, args.parallel),
    )

    # Calculate ratio safely to avoid division by zero
    ratio = total_included_n / total_lines_n if total_lines_n > 0 else 0.0
    logger.info(f'Filter statistics: {total_included_n} / {total_lines_n} lines ({ratio:.4f})')
    logger.info(f'Errors: {total_errors_n}')

    if limit > 0 and total_included_n >= 0:
        logger.info(f'Limit reached at {limit}')

    # End timing and log execution time
    end_time = time.time()
    execution_time = end_time - start_time

    logger.info(f'Script execution time: {execution_time:.3f} seconds')