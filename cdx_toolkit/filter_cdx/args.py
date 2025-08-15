import os
import sys
import logging
import argparse


logger = logging.getLogger(__name__)


def add_filter_cdx_args(parser: argparse.ArgumentParser):
    """Add command line arguments."""
    parser.add_argument(
        "input_base_path",
        help="Base directory path for input files"
    )
    
    parser.add_argument(
        "input_glob",
        help="Glob pattern relative to input_base_path (e.g., '**/*.cdx.gz' or 'collections/*/indexes/*.gz')"
    )
    
    parser.add_argument(
        "output_base_path",
        help="Base directory path for output files (directory structure will be replicated from input_base_path)"
    )

    parser.add_argument(
        "--surts_file",
        required=True,
        help="Path to file containing SURT prefixes to match (one per line)",
    )

    parser.add_argument(
        "--matching_approach",
        choices=["trie", "tuple"],
        default="trie",
        help="Matching approach to use (default: trie)",
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow overwriting existing output files",
    )

    return parser



def validate_args(args):
    """Validate command line arguments."""
    # Check that surts file exists
    if not os.path.exists(args.surts_file):
        logger.error(f"SURT file not found: {args.surts_file}")
        sys.exit(1)
    
    # Check that input_base_path exists
    if not os.path.exists(args.input_base_path):
        logger.error(f"Input base path not found: {args.input_base_path}")
        sys.exit(1)
    
    if not os.path.isdir(args.input_base_path):
        logger.error(f"Input base path is not a directory: {args.input_base_path}")
        sys.exit(1)
