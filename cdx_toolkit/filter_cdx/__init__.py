
import logging
import time
import os
import sys
import glob

from cdx_toolkit.filter_cdx.args import validate_args
from cdx_toolkit.filter_cdx.matcher import TupleMatcher, TrieMatcher

try:
    import smart_open
    smart_open_installed = True
except ImportError:
    smart_open_installed = True

logger = logging.getLogger(__name__)

def run_filter_cdx(args, cmdline: str):
    """Filter CDX index files based on a given SURT whitelist. 

    - A index entry's SURT must start with one of the SURTs from the whiteliste to be considered.
    - All other index entries are discarded.
    - All input/output paths can be local or remote paths (S3, ...) and compressed (*.gz).
    """

    validate_args(args)
    
    # Resolve input and output paths using glob pattern
    # TODO this should support glob via S3 (e.g., to fetch the indices from s3://commoncrawl/cc-index/collections/* ...)
    input_paths, output_paths = resolve_paths(args)
    validate_resolved_paths(output_paths, args.overwrite)

    logger.info("Filtering CDX files based on whitelist")
    logger.info(f"Found {len(input_paths)} files matching pattern: {os.path.join(args.input_base_path, args.input_glob)}")
    
    # Ensure output directories exist
    # TODO make sure this works with remote paths as well!
    ensure_output_directories(output_paths)
    
    # Start timing
    start_time = time.time()

    # Load SURT prefixes
    with optional_smart_open(args.surts_file) as input_f:
        include_surt_prefixes = [line.strip() for line in input_f.readlines()]

    # Create matcher based on selected approach
    matcher_classes = {
        "trie": TrieMatcher,
        "tuple": TupleMatcher,
    }

    matcher = matcher_classes[args.matching_approach](include_surt_prefixes)

    logger.info(
        f"Loaded {len(include_surt_prefixes):,} surts using {args.matching_approach} approach"
    )

    # Process each input/output file pair
    total_lines_n = 0
    total_included_n = 0

    for input_path, output_path in zip(input_paths, output_paths):
        logger.info("Reading index from %s", input_path)
        logger.info("Writing filter output to %s", output_path)

        lines_n = 0
        included_n = 0

        with optional_smart_open(output_path, "w") as output_f:
            with optional_smart_open(input_path) as input_f:
                for i, line in enumerate(input_f):
                    surt_length = line.find(
                        " "
                    )  # we do not need to parse the full line
                    record_surt = line[:surt_length]
                    lines_n += 1

                    # Use matcher
                    include_record = matcher.matches(record_surt)

                    if include_record:
                        output_f.write(line)
                        included_n += 1

                    if (i % 100_000) == 0:
                        logger.info(f"Lines completed: {i:,}")

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

    logger.info(
        f"Script execution time: {execution_time:.3f} seconds"
    )

def optional_smart_open(*args, **kwargs):
    """Helper function to make `smart_open` an optional dependency."""
    if smart_open_installed:
        return smart_open.open(*args, **kwargs)
    else:
        return open(*args, **kwargs)
    
def resolve_paths(args):
    """Resolve input paths from glob pattern and generate corresponding output paths."""
    # Construct full glob pattern
    full_glob_pattern = os.path.join(args.input_base_path, args.input_glob)
    
    # Get input files from glob pattern
    input_files = glob.glob(full_glob_pattern, recursive=True)
    if not input_files:
        logger.error(f"No files found matching glob pattern: {full_glob_pattern}")
        sys.exit(1)
    
    # Sort for consistent ordering
    input_files.sort()
    
    # Generate corresponding output paths
    output_files = []
    for input_path in input_files:
        # Get relative path from input_base_path
        rel_path = os.path.relpath(input_path, args.input_base_path)
        
        # Create corresponding output path
        output_path = os.path.join(args.output_base_path, rel_path)
        output_files.append(output_path)
    
    return input_files, output_files


def ensure_output_directories(output_paths):
    """Ensure all output directories exist, creating them if necessary."""
    created_dirs = set()
    for output_path in output_paths:
        output_dir = os.path.dirname(output_path)
        if output_dir and output_dir not in created_dirs:
            os.makedirs(output_dir, exist_ok=True)
            created_dirs.add(output_dir)
    
    if created_dirs:
        logger.info(f"Created {len(created_dirs)} output directories")



def validate_resolved_paths(output_paths, overwrite):
    """Validate resolved output paths."""
    # Check if output files exist and overwrite flag
    if not overwrite:
        for output_path in output_paths:
            if os.path.exists(output_path):
                logger.error(
                    f"Output file already exists: {output_path}. "
                    "Use --overwrite to overwrite existing files."
                )
                sys.exit(1)

