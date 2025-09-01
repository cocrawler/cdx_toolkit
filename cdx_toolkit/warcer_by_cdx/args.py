import logging
import argparse


logger = logging.getLogger(__name__)


def add_warcer_by_cdx_args(parser: argparse.ArgumentParser):
    parser.add_argument(
        "index_path", help="Path to CDX index file (local or remote, e.g. S3)"
    )
    parser.add_argument(
        "--index-glob",
        type=str,
        default=None,
        help="a glob pattern for read from multiple indices",
    )
    parser.add_argument("--prefix", default="TEST", help="prefix for the warc filename")
    parser.add_argument(
        "--subprefix",
        type=str,
        default=None,
        help="subprefix for the warc filename, default None",
    )
    parser.add_argument(
        "--size",
        type=int,
        default=1000000000,
        help="target for the warc filesize in bytes",
    )
    parser.add_argument(
        "--creator",
        action="store",
        help="creator of the warc: person, organization, service",
    )
    parser.add_argument(
        "--operator", action="store", help="a person, if the creator is an organization"
    )
    parser.add_argument(
        "--warc-download-prefix",
        action="store",
        help="prefix for downloading content, automatically set for CC",
    )
    parser.add_argument(
        "--write-index-as-record",
        action="store_true",
        help="If enable, the CDX index is written as resource record to the WARC file",
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        help="Number of parallel workers for fetchin WARC records (default: 1, sequential processing)",
    )
    parser.add_argument(
        "--implementation", type=str, default="fsspec", help="implementation (fsspec, aioboto3)"
    )
    return parser
