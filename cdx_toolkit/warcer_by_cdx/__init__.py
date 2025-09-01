import logging
import sys
import time
from typing import Literal

import fsspec


from cdx_toolkit.utils import get_version, setup
from cdx_toolkit.warcer_by_cdx.aioboto3_warcer import filter_warc_by_cdx_via_aioboto3
from cdx_toolkit.warcer_by_cdx.fsspec_warcer import filter_warc_by_cdx_via_fsspec


logger = logging.getLogger(__name__)


def run_warcer_by_cdx(args, cmdline):
    """Like warcer but fetches WARC records based on one or more CDX index files.

    The CDX files can be filtered using the `filter_cdx` commands based a given URL/SURT list.

    Approach:
    - Iterate over one or more CDX files to extract capture object (file, offset, length)
    - Fetch WARC record based on capture object
    - Write to new WARC file with metadata including resource record with index.
    - The CDX resource record is written to the WARC directly before for response records that matches to the CDX.
    """
    logger.info("Filtering WARC files based on CDX")

    cdx, kwargs = setup(args)

    # Start timing
    start_time = time.time()

    implementation = args.implementation

    write_index_as_record = args.write_index_as_record
    ispartof = args.prefix
    if args.subprefix:
        ispartof += "-" + args.subprefix

    info = {
        "software": "pypi_cdx_toolkit/" + get_version(),
        "isPartOf": ispartof,
        "description": "warc extraction based on CDX generated with: " + cmdline,
        "format": "WARC file version 1.0",
    }
    if args.creator:
        info["creator"] = args.creator
    if args.operator:
        info["operator"] = args.operator

    writer_kwargs = {}
    if "size" in kwargs:
        writer_kwargs["size"] = kwargs["size"]
        del kwargs["size"]

    n_parallel = args.parallel
    log_every_n = 5
    limit = 0 if args.limit is None else args.limit
    prefix_path = str(args.prefix)
    prefix_fs, prefix_fs_path =  fsspec.url_to_fs(prefix_path)
    
    # make sure the base dir exists
    prefix_fs.makedirs(prefix_fs._parent(prefix_fs_path), exist_ok=True)

    index_paths = get_index_paths(
        args.index_path,
        args.index_glob,
    )

    if implementation == "fsspec":
        records_n = filter_warc_by_cdx_via_fsspec(
            index_paths=index_paths,
            prefix_path=prefix_path,
            writer_info=info,
            writer_subprefix=args.subprefix,
            write_index_as_record=write_index_as_record,
            limit = limit,
            log_every_n = log_every_n,
            warc_download_prefix = cdx.warc_download_prefix,
            n_parallel=n_parallel,
            writer_kwargs=writer_kwargs,
        )
    elif implementation == "aioboto3":
        records_n = filter_warc_by_cdx_via_aioboto3(
            index_paths=index_paths,
            prefix_path=prefix_path,
            writer_info=info,
            writer_subprefix=args.subprefix,
            write_index_as_record=write_index_as_record,
            limit = limit,
            log_every_n = log_every_n,
            warc_download_prefix = cdx.warc_download_prefix,
            n_parallel=n_parallel,
            writer_kwargs=writer_kwargs,
        )
    else:
        raise ValueError("Invalid implementation")

    logger.info("WARC records extracted: %i", records_n)

    # End timing and log execution time
    end_time = time.time()
    execution_time = end_time - start_time

    logger.info(f"Script execution time: {execution_time:.3f} seconds")

def get_index_paths(index_path: str, index_glob: str | None = None) -> list[str]:
    if index_glob is None:
        # Read from a single index
        index_paths = [index_path]
    else:
        # Prepare index paths
        index_fs, index_fs_path = fsspec.url_to_fs(index_path)

        # Fetch multiple indicies via glob
        full_glob = index_fs_path + index_glob

        logger.info("glob pattern from %s (%s)", full_glob, index_fs.protocol)

        index_paths = sorted(index_fs.glob(full_glob))

        logger.info(
            "glob pattern found %i index files in %s", len(index_paths), index_fs_path
        )

        if not index_paths:
            logger.error("no index files found via glob")
            sys.exit(1)

    return index_paths
