import logging
import argparse


logger = logging.getLogger(__name__)


def add_warcer_by_cdx_args(parser: argparse.ArgumentParser):
    parser.add_argument('cdx_path', help='Path to CDX index file (local or remote, e.g. S3)')
    parser.add_argument(
        '--cdx-glob',
        type=str,
        default=None,
        help='a glob pattern for read from multiple CDX indices',
    )
    parser.add_argument('--prefix', default='TEST', help='prefix for the warc filename')
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
    )
    parser.add_argument(
        '--write-paths-as-resource-records',  # --write-index-as-record
        nargs="*",
        help='Paths to multiple files. File content is written to as a resource record to each the WARC file',
    )
    parser.add_argument(
        '--write-paths-as-resource-records-metadata',
        nargs="*",
        help='Paths to multiple metadata files (JSON) for resource records from `--write-paths-as-resource-records`',
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
    return parser
