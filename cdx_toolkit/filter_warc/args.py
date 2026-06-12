import logging
import argparse


logger = logging.getLogger(__name__)


def add_warcer_by_cdx_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        '--cdx-path',
        type=str,
        default=None,
        help='Path to CDX index file (local or remote, e.g. S3). Required if target source is set to `cdx`.',
    )
    parser.add_argument(
        '--cdx-glob',
        type=str,
        default=None,
        help='a glob pattern for read from multiple CDX indices',
    )
    parser.add_argument(
        '--athena-hostnames',
        type=str,
        nargs="+",
        default=None,
        help=('Hostnames to filter for via Athena (whitelist). Use this OR --athena-query/'
              '--athena-query-file (mutually exclusive) when target source is `athena`.'),
    )
    parser.add_argument(
        '--athena-query',
        type=str,
        default=None,
        help=('Raw Athena SQL to run instead of the hostname-based query (power users). The query '
              'must SELECT the columns warc_filename, warc_record_offset, warc_record_length. '
              'Mutually exclusive with --athena-hostnames and --athena-query-file.'),
    )
    parser.add_argument(
        '--athena-query-file',
        type=str,
        default=None,
        help='Path to a file containing the raw Athena SQL (alternative to --athena-query).',
    )
    parser.add_argument(
        '--athena-database',
        type=str,
        default=None,
        help='Athena database. Required if target source is set to `athena`.',
    )
    parser.add_argument(
        '--athena-s3-output',
        type=str,
        default=None,
        help='Athena S3 output location. Required if target source is set to `athena`.',
    )
    parser.add_argument(
        '--confirm-athena-cost',
        action='store_true',
        help=('Skip the Athena cost-confirmation prompt and run even unpartitioned / large-scan '
              'queries. Athena bills per TB scanned; restrict with --crawl to reduce cost.'),
    )
    parser.add_argument('--prefix', default='TEST', help='prefix for the output warc filename')
    parser.add_argument(
        '--subprefix',
        type=str,
        default=None,
        help='subprefix for the warc filename, default None',
    )
    parser.add_argument(
        '--size',
        type=int,
        default=1000000000,
        help='target for the warc filesize in bytes',
    )
    parser.add_argument(
        '--creator',
        action='store',
        help='creator of the warc: person, organization, service',
    )
    parser.add_argument('--operator', action='store', help='a person, if the creator is an organization')
    parser.add_argument(
        '--description',
        action='store',
        help='the `description` field in the `warcinfo` record (auto-generated if not set)',
    )
    parser.add_argument(
        '--is-part-of',
        action='store',
        help='the `isPartOf` field in the `warcinfo` record (auto-generated if not set)',
    )
    parser.add_argument(
        '--warc-download-prefix',
        action='store',
        help='prefix for downloading content, automatically set for CC',
        default='https://data.commoncrawl.org',
    )
    parser.add_argument(
        '--write-paths-as-metadata-records',
        nargs='*',
        help='Paths to multiple files. File content is written to as a metadata record to each the WARC file',
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=1,
        help='Number of parallel workers for reading and writing WARC records (default: 1, sequential processing)',
    )
    parser.add_argument(
        '--parallel_readers',
        type=int,
        default=None,
        help='Number of parallel workers for reading WARC records (default: same as `parallel`)',
    )
    parser.add_argument(
        '--parallel_writers',
        type=int,
        default=None,
        help='Number of parallel workers for writing WARC records (default: same as `parallel`)',
    )
    parser.add_argument(
        '--log_every_n',
        type=int,
        default=1000,
        help='Every N extracted record a log message is emitted (0 = no record logs)',
    )
    parser.add_argument(
        '--target-source',
        action='store',
        default='cdx',
        help=('Source from that the filter targets are loaded (available options: `cdx`, `athena`; '
              'defaults to `cdx`). For `athena`, use the global --crawl to restrict the scan to '
              'specific crawls (strongly recommended; Athena bills per TB scanned).'),
    )
    return parser
