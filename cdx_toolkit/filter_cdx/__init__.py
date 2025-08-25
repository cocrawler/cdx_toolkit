import logging
import os
import time
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial

import fsspec
from surt import surt

from cdx_toolkit.filter_cdx.matcher import TupleMatcher, TrieMatcher


logger = logging.getLogger(__name__)


def run_filter_cdx(args, cmdline: str):
    """Filter CDX index files based on a given URL or SURT whitelist.

    - If a URL filter is provided, it is converted to a SURT filter.
    - A index entry's SURT must start with one of the SURTs from the whitelist to be considered.
    - All other index entries are discarded.
    - All input/output paths can be local or remote paths (S3, ...) and compressed (*.gz).
    """
    logger.info("Filtering CDX files based on whitelist")

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

    logger.info(
        f"Found {len(input_paths)} files matching pattern: {args.input_base_path}/{args.input_glob}"
    )

    # Load URL or SURT prefixes from file (each line is a surt)
    filter_fs, filter_fs_path = fsspec.url_to_fs(args.filter_file)
    logger.info("Loading whitelist from %s", filter_fs_path)

    if not filter_fs.exists(filter_fs_path):  # Check that surts file exists
        logger.error(f"Filter file not found: {filter_fs_path}")
        sys.exit(1)

    with filter_fs.open(filter_fs_path, "rt") as input_f:
        include_prefixes = [line.strip() for line in input_f.readlines()]

    # Convert to SURT if filter file contains URLs 
    if args.filter_type == "url":
        logger.info("Converting urls to surts ...")
        include_surt_prefixes = [surt(url) for url in include_prefixes]
    else:
        # Filter is already given as surts
        include_surt_prefixes = include_prefixes

    # Create matcher based on selected approach
    matcher_classes = {
        "trie": TrieMatcher,
        "tuple": TupleMatcher,
    }

    matcher = matcher_classes[args.matching_approach](include_surt_prefixes)

    logger.info(
        f"Loaded {len(include_surt_prefixes):,} filter entries using {args.matching_approach} approach"
    )

    # Process files in parallel or sequentially
    n_parallel = args.parallel
    total_lines_n = 0
    total_included_n = 0
    
    if n_parallel > 1:
        # Parallel processing
        logger.info("Parallel processes: %i", n_parallel)
        with ProcessPoolExecutor(max_workers=n_parallel) as executor:
            # Create partial function with common arguments
            process_file_partial = partial(
                _process_single_file,
                matcher=matcher,
                limit=args.limit if hasattr(args, 'limit') else 0
            )
            
            # Submit all jobs
            future_to_paths = {
                executor.submit(process_file_partial, input_path, output_path): (input_path, output_path)
                for input_path, output_path in zip(input_paths, output_paths)
            }
            
            # Collect results
            for future in as_completed(future_to_paths):
                input_path, output_path = future_to_paths[future]
                try:
                    lines_n, included_n = future.result()
                    logger.info(
                        f"File statistics for {input_path}: included_n={included_n}; lines_n={lines_n}; ratio={included_n/lines_n:.4f}"
                    )
                    total_lines_n += lines_n
                    total_included_n += included_n
                except Exception as exc:
                    logger.error(f"File {input_path} generated an exception: {exc}")
    else:
        # Sequential processing
        logger.info("Sequential processing")
        for input_path, output_path in zip(input_paths, output_paths):
            lines_n, included_n = _process_single_file(
                input_path, output_path, matcher, args.limit if hasattr(args, 'limit') else 0
            )
            logger.info(
                f"File statistics for {input_path}: included_n={included_n}; lines_n={lines_n}; ratio={included_n/lines_n:.4f}"
            )
            total_lines_n += lines_n
            total_included_n += included_n

    logger.info(
        f"Total statistics: included_n={total_included_n}; lines_n={total_lines_n}; ratio={total_included_n/total_lines_n:.4f}"
    )

    # End timing and log execution time
    end_time = time.time()
    execution_time = end_time - start_time

    logger.info(f"Script execution time: {execution_time:.3f} seconds")


def resolve_paths(input_base_path: str, input_glob: str, output_base_path: str):
    """Resolve input paths from glob pattern and generate corresponding output paths."""
    # Use fsspec to handle local and remote file systems
    input_fs, input_fs_base_path = fsspec.url_to_fs(input_base_path)
    input_full_glob = input_fs_base_path + input_glob

    # Get input files from glob pattern
    input_fs_file_paths = sorted(input_fs.glob(input_full_glob))
    if not input_fs_file_paths:
        logger.error(f"No files found matching glob pattern: {input_full_glob}")
        sys.exit(1)

    # Generate corresponding output paths
    output_file_paths = []
    input_file_paths = []
    for input_path in input_fs_file_paths:
        # Get relative path from input_base_path without last slash
        rel_path = input_path[len(input_fs_base_path) + 1 :]

        # Create corresponding full input and output path
        output_file_paths.append(os.path.join(output_base_path, rel_path))
        input_file_paths.append(os.path.join(input_base_path, rel_path))

    return input_file_paths, output_file_paths


def _process_single_file(input_path, output_path, matcher, limit: int = 0, log_every_n: int = 100_000):
    """Process a single input/output file pair. Returns (lines_n, included_n)."""
    lines_n = 0
    included_n = 0
    
    logger.info("Reading index from %s", input_path)
    logger.info("Writing filter output to %s", output_path)
    
    # Input/output from local or remote file system
    input_fs, input_fs_path = fsspec.url_to_fs(input_path)
    output_fs, output_fs_path = fsspec.url_to_fs(output_path)
    
    # Make sure output directory exists
    output_fs.makedirs(output_fs._parent(output_fs_path), exist_ok=True)
    
    # Read and write compressed file if needed
    compression = "gzip" if input_fs_path.endswith(".gz") else None
    
    with output_fs.open(output_fs_path, "w", compression=compression) as output_f:
        with input_fs.open(input_fs_path, "rt", compression=compression) as input_f:
            for i, line in enumerate(input_f, 1):
                # Read CDX line
                surt_length = line.find(" ")  # we do not need to parse the full line
                record_surt = line[:surt_length]
                lines_n += 1
                
                # Use matcher
                include_record = matcher.matches(record_surt)
                
                if include_record:
                    output_f.write(line)
                    included_n += 1
                    
                    if limit > 0 and included_n >= limit:
                        logger.info("Limit reached at %i from %s", limit, input_path)
                        break
                
                if (i % log_every_n) == 0:
                    logger.info(f"Lines completed: {i:,} (matched: {included_n:,}) from {input_path}")
    
    return lines_n, included_n


def validate_resolved_paths(output_paths, overwrite):
    """Validate resolved output paths and create directories if needed."""
    # Check if output files exist and overwrite flag
    if not overwrite:
        output_fs, _ = fsspec.url_to_fs(output_paths[0])
        for output_path in output_paths:
            if output_fs.exists(output_path):
                logger.error(
                    f"Output file already exists: {output_path}. "
                    "Use --overwrite to overwrite existing files."
                )
                sys.exit(1)

            # Make sure directory exists
            output_fs.makedirs(output_fs._parent(output_path), exist_ok=True)
