import json
from pathlib import Path

from typing import Iterable, Optional, Tuple, Union

import fsspec
import logging


logger = logging.getLogger(__name__)


def get_index_as_string_from_path(
    index_path: Union[str, Path], index_fs: Optional[fsspec.AbstractFileSystem] = None
) -> str:
    """Fetch (and decompress) index content as string from local or remote path."""
    logger.info('Fetching index from %s ...', index_path)
    if index_fs is None:
        index_fs, index_fs_path = fsspec.url_to_fs(index_path)
    else:
        index_fs_path = index_path

    compression = 'gzip' if index_fs_path.endswith('.gz') else None

    with index_fs.open(index_fs_path, 'rt', compression=compression) as f:
        return f.read()


def read_cdx_line(line: str, warc_download_prefix: str) -> Tuple[str, int, int]:
    cols = line.split(' ', maxsplit=2)

    if len(cols) == 3:
        # NOTE: We assume the following format (CC-CDX format): <surt> <timestamp> <json_data>
        #
        # IA follows a different CDX specification from https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
        # > The default first line of a CDX file is:
        # > CDX A b e a m s c k r V v D d g M n
        data = json.loads(cols[2])
        data['timestamp'] = cols[1]
    else:
        raise ValueError(f'Cannot parse line: {line}')

    filename = data['filename']
    offset = int(data['offset'])
    length = int(data['length'])

    warc_url = warc_download_prefix + '/' + filename

    return (warc_url, offset, length)


def iter_cdx_index_from_path(index_path: str, warc_download_prefix: str) -> Iterable[Tuple[str, int, int]]:
    """
    Iterate CDX records from a file path (gzipped; local or remote).
    """
    logger.info('Reading CDX from %s', index_path)

    with fsspec.open(index_path, 'rt', compression='gzip' if index_path.endswith('.gz') else None) as f:
        for line in f:
            try:
                yield read_cdx_line(line, warc_download_prefix)
            except Exception:
                # Skip malformed lines
                logger.error('Invalid CDX line: %s', line)
                continue

    logger.info(f'CDX completed from {index_path}')
