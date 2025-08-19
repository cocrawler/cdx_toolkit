
import json
import logging
import sys
from typing import Iterable

import fsspec

import cdx_toolkit
from cdx_toolkit.utils import get_version, setup


LOGGER = logging.getLogger(__name__)


def run_warcer_by_cdx(cmd, cmdline):
    """Like warcer but fetches WARC records based on an CDX index file.
    
    Approach:
    - Iterate over CDX file to extract capture object (file, offset, length)
    - Fetch WARC record based on capture object
    - Write to new WARC file with metadata
    """
    cdx, kwargs = setup(cmd)

    ispartof = cmd.prefix
    if cmd.subprefix:
        ispartof += '-' + cmd.subprefix

    info = {
        'software': 'pypi_cdx_toolkit/'+get_version(),
        'isPartOf': ispartof,
        'description': 'warc extraction generated with: '+cmdline,
        'format': 'WARC file version 1.0',  # todo: if we directly read a warc, have this match the warc
        # TODO add information from the index file
    }
    if cmd.creator:
        info['creator'] = cmd.creator
    if cmd.operator:
        info['operator'] = cmd.operator

    kwargs_writer = {}
    if 'size' in kwargs:
        kwargs_writer['size'] = kwargs['size']
        del kwargs['size']

    writer = cdx_toolkit.warc.get_writer(cmd.prefix, cmd.subprefix, info, **kwargs_writer)

    # TODO probably we should support multiple indices as input

    if cmd.index_glob is None:
        # Read from a single index
        index_paths = [cmd.index_path]
    else:
        # Fetch multiple indicies via glob
        index_fs, index_fs_path = fsspec.url_to_fs(cmd.index_path)
        index_paths = sorted(index_fs.glob(cmd.index_glob))

        LOGGER.info('glob pattern found %i index files in %s', len(index_paths), index_fs_path)

        if not index_paths:
            LOGGER.error('no index files found')
            sys.exit(1)
            
    # Iterate over index files
    for index_path in index_paths:
        LOGGER.info('filtering based on index from %s', index_path)

        # The index file holds all the information to download specific objects (file, offset, length etc.)
        for obj in get_caputure_objects_from_index_file(index_path=index_path, warc_download_prefix=cmd.warc_download_prefix):
            url = obj['url']

            timestamp = obj['timestamp']
            try:
                record = obj.fetch_warc_record()
            except RuntimeError:  # pragma: no cover
                LOGGER.warning('skipping capture for RuntimeError 404: %s %s', url, timestamp)
                continue
            if obj.is_revisit():
                LOGGER.warning('revisit record being resolved for url %s %s', url, timestamp)
            writer.write_record(record)

        LOGGER.info('filtering completed (index: %s)', index_path)

def get_caputure_objects_from_index_file(index_path: str, warc_download_prefix=None) -> Iterable[cdx_toolkit.CaptureObject]:
    """Read CDX index file and generate CaptureObject objects."""
    index_fs, index_fs_path = fsspec.url_to_fs(index_path)
    
    with index_fs.open(index_fs_path) as f:
        for line in enumerate(f):
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
