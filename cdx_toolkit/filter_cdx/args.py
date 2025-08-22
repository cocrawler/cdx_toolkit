import argparse


def add_filter_cdx_args(parser: argparse.ArgumentParser):
    """Add command line arguments."""
    parser.add_argument(
        "input_base_path",
        help="Base directory path or remote URL for one or multiple input files (e.g., URL to S3 bucket)",
    )
    parser.add_argument(
        "surts_file",
        help="Path to file containing SURT prefixes to match (one per line)",
    )
    parser.add_argument(
        "output_base_path",
        help="Base directory path for output files (directory structure will be replicated from input_base_path)",
    )
    parser.add_argument(
        "--input_glob",
        help="Glob pattern relative to input_base_path (e.g., '**/*.cdx.gz' or 'collections/*/indexes/*.gz')",
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
