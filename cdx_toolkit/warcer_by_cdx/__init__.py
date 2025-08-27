from io import BytesIO
import json
import logging
import os
from pathlib import Path
import sys
from typing import Iterable

import fsspec


from tqdm import tqdm
from warcio import WARCWriter
from warcio.recordloader import ArcWarcRecord

import cdx_toolkit
from cdx_toolkit.utils import get_version, setup


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
    cdx, kwargs = setup(args)

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

    kwargs_writer = {}
    if "size" in kwargs:
        kwargs_writer["size"] = kwargs["size"]
        del kwargs["size"]

    log_every_n = 10_000
    limit = 0 if args.limit is None else args.limit
    prefix_path = Path(args.prefix)

    # make sure the base dir exists
    os.makedirs(prefix_path.parent, exist_ok=True)

    writer = cdx_toolkit.warc.get_writer(
        str(prefix_path), args.subprefix, info, **kwargs_writer
    )

    # Prepare index paths
    index_fs, index_fs_path = fsspec.url_to_fs(args.index_path)

    if args.index_glob is None:
        # Read from a single index
        index_paths = [args.index_path]
    else:
        # Fetch multiple indicies via glob
        full_glob = index_fs_path + args.index_glob

        logger.info("glob pattern from %s (%s)", full_glob, index_fs.protocol)

        index_paths = sorted(index_fs.glob(full_glob))

        logger.info(
            "glob pattern found %i index files in %s", len(index_paths), index_fs_path
        )

        if not index_paths:
            logger.error("no index files found")
            sys.exit(1)

    # Iterate over index files
    records_n = 0
    for index_path in index_paths:
        logger.info("filtering based on index from %s (%s)", index_path, index_fs.protocol)

        # Read index completely (for the WARC resource record)
        index = get_index_from_path(index_path, index_fs=index_fs)

        if not index:
            # skip empty indicies
            continue

        # Write index as record to WARC
        # TODO at what position should the resource records be written?
        writer.write_record(get_index_record(index, index_path))

        logger.info("index resource recorded added")

        # The index file holds all the information to download specific objects (file, offset, length etc.)
        for obj in generate_caputure_objects_from_index(
            index=index, warc_download_prefix=cdx.warc_download_prefix
        ):
            url = obj["url"]
            timestamp = obj["timestamp"]

            try:
                record = obj.fetch_warc_record()
            except RuntimeError:  # pragma: no cover
                logger.warning(
                    "skipping capture for RuntimeError 404: %s %s", url, timestamp
                )
                continue
            if obj.is_revisit():
                logger.warning(
                    "revisit record being resolved for url %s %s", url, timestamp
                )
            writer.write_record(record)
            records_n += 1

            if (records_n % log_every_n) == 0:
                logger.info(f"Records completed: {records_n:,} from {index_path}")

            if limit > 0 and records_n >= limit:
                logger.info("Limit reached at %i", limit)
                break

        if limit > 0 and records_n >= limit:
            # stop index loop
            break

        logger.info("Filtering completed (index file: %s)", index_path)

    writer.close()

    logger.info("WARC records extracted: %i", records_n)


def get_index_from_path(index_path: str | Path, index_fs: None | fsspec.AbstractFileSystem = None) -> str:
    """Fetch (and decompress) index content as string from local or remote path."""
    if index_fs is None:
        index_fs, index_fs_path = fsspec.url_to_fs(index_path)
    else:
        index_fs_path = index_path

    compression = "gzip" if index_fs_path.endswith(".gz") else None

    with index_fs.open(index_fs_path, "rt", compression=compression) as f:
        return f.read()


def get_index_record(
    index: str, index_path: str, encoding: str = "utf-8"
) -> ArcWarcRecord:
    """Build WARC resource record for index."""
    return WARCWriter(None).create_warc_record(
        uri=index_path,  # TODO this could be a local / internal path
        record_type="resource",
        payload=BytesIO(index.encode(encoding)),
        http_headers=None,
        warc_content_type="application/cdx",
        warc_headers_dict=None,  # TODO should we add some other metadata headers?
    )


def generate_caputure_objects_from_index(
    index: str, warc_download_prefix=None, limit: int = 0, progress_bar: bool = True
) -> Iterable[cdx_toolkit.CaptureObject]:
    """Read CDX index and generate CaptureObject objects."""
    index_lines = index.splitlines()

    # if progress_bar:
    #     index_lines = tqdm(index_lines, desc="Extracting from WARC", total=len(index_lines))

    for i, line in enumerate(tqdm(index_lines, desc="Extracting from WARC", total=len(index_lines)), 1):
        cols = line.split(" ", maxsplit=2)

        if len(cols) == 3:
            # TODO can there be a different format?
            # surt, timestamp, json_data = cols
            data = json.loads(cols[2])
            data["timestamp"] = cols[1]
        else:
            raise ValueError(f"Cannot parse line: {line}")

        yield cdx_toolkit.CaptureObject(
            data=data, wb=None, warc_download_prefix=warc_download_prefix
        )

        if limit > 0 and i >= limit:
            break
