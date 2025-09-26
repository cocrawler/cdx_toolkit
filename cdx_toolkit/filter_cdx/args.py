import argparse


def add_filter_cdx_args(parser: argparse.ArgumentParser):
    """Add command line arguments."""
    parser.add_argument(
        'input_base_path',
        help='Base directory path on the local file system or remote URL for one or multiple CDX files (e.g., URL to S3 bucket)',
    )
    parser.add_argument(
        'filter_file',
        help='Path to file containing URL or SURT prefixes to filter for (one per line)',
    )
    parser.add_argument(
        'output_base_path',
        help='Base directory path for output files (directory structure will be replicated from input_base_path)',
    )
    parser.add_argument(
        '--filter-type',
        type=str,
        default='url',
        help='Type of filter entries (options: `url` or `surt`, defaults to `url`)',
    )
    parser.add_argument(
        '--input-glob',
        help="Glob pattern relative to input_base_path (e.g., '**/*.cdx.gz' or 'collections/*/indexes/*.gz')",
    )
    parser.add_argument(
        '--matching-approach',
        choices=['trie', 'tuple'],
        default='trie',
        help='Matching approach to use (default: trie)',
    )
    parser.add_argument(
        '--overwrite',
        action='store_true',
        help='Allow overwriting existing output files',
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=1,
        help='Number of parallel workers for processing multiple input files (default: 1, sequential processing)',
    )

    return parser
