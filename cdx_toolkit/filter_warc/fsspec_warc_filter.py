import json
import logging
from typing import Dict, Iterable, List, Optional

import cdx_toolkit
from concurrent.futures import ThreadPoolExecutor, as_completed

from warcio.recordloader import ArcWarcRecord

from cdx_toolkit.filter_warc.cdx_utils import get_index_as_string_from_path
from cdx_toolkit.filter_warc.warc_utils import get_resource_record_from_path


logger = logging.getLogger(__name__)


def filter_warc_by_cdx_via_fsspec(
    index_paths: List[str],
    prefix_path: str,
    writer_info: Dict,
    writer_subprefix: Optional[str] = None,
    write_paths_as_resource_records: Optional[List[str]] = None,
    write_paths_as_resource_records_metadata: Optional[List[str]] = None,
    limit: int = 0,
    log_every_n: int = 1000,
    warc_download_prefix: Optional[str] = None,
    n_parallel: int = 1,
    writer_kwargs: Optional[Dict] = None,
) -> int:
    writer = cdx_toolkit.warc.get_writer(
        prefix_path,
        writer_subprefix,
        writer_info,
        **(writer_kwargs if writer_kwargs else {}),
    )

    # Iterate over index files
    records_n = 0
    for index_path in index_paths:
        logger.info('Filtering WARC based on CDX from %s', index_path)

        # Read index completely (for the WARC resource record)
        index = get_index_as_string_from_path(index_path)

        if not index:
            # skip empty indicies
            continue

        # Write file content from paths as resource records to WARC
        if write_paths_as_resource_records:
            logger.info('Writing resource records to WARC ... ')

            # Resource records are written at the beginning the WARC file.
            for i, resource_record_path in enumerate(write_paths_as_resource_records):
                logger.info(f'Writing resource record from {resource_record_path} ...')
                resource_record = get_resource_record_from_path(
                    file_path=resource_record_path,
                    metadata_path=(
                        write_paths_as_resource_records_metadata[i]
                        if write_paths_as_resource_records_metadata
                        else None
                    ),
                )
                writer.write_record(resource_record)
            
            logger.info(f'Resource records added: {len(write_paths_as_resource_records)}')

        # The index file holds all the information to download specific objects (file, offset, length etc.)
        index_lines = index.splitlines()
        index_limit = limit - records_n

        if index_limit > 0:
            index_lines = index_lines[:index_limit]

        records_gen = fetch_records_from_index(
            index_lines=index_lines, warc_download_prefix=warc_download_prefix, n_parallel=n_parallel
        )
        # records_gen = tqdm(fetch_records_from_index(
        #     index_lines=index_lines, warc_download_prefix=cdx.warc_download_prefix, n_parallel=n_parallel
        # ), desc="Fetch and write WARC", total=len(index_lines))

        for record in records_gen:
            writer.write_record(record)
            records_n += 1

            if (records_n % log_every_n) == 0:
                logger.info(f'Record progress: {records_n:,} from {index_path}')

        if limit > 0 and records_n >= limit:
            # stop index loop
            logger.info('Limit reached')
            break

        logger.info('Filtering completed (index file: %s)', index_path)

    writer.close()

    return records_n


def fetch_single_record(obj):
    """Fetch a single WARC record with error handling."""
    url = obj['url']
    timestamp = obj['timestamp']

    try:
        record = obj.fetch_warc_record()
        if obj.is_revisit():
            logger.warning('revisit record being resolved for url %s %s', url, timestamp)
        return record
    except RuntimeError:  # pragma: no cover
        logger.warning('skipping capture for RuntimeError 404: %s %s', url, timestamp)
        return None


def fetch_records_from_index(
    index_lines: List[str], warc_download_prefix=None, limit: int = 0, n_parallel: int = 1
) -> Iterable[ArcWarcRecord]:
    """Fetch WARC records based on CDX index."""

    if n_parallel <= 1:
        # Sequential processing
        for obj in generate_caputure_objects_from_index(
            index_lines=index_lines,
            warc_download_prefix=warc_download_prefix,
            limit=limit,
        ):
            record = fetch_single_record(obj)
            if record is not None:
                yield record
    else:
        # Parallel processing
        logger.info(f'Fetch records in parallel with {n_parallel=}')
        objects = list(
            generate_caputure_objects_from_index(
                index_lines=index_lines,
                warc_download_prefix=warc_download_prefix,
                limit=limit,
            )
        )  # TODO this loads all records into memory

        with ThreadPoolExecutor(max_workers=n_parallel) as executor:
            # Submit all tasks
            future_to_obj = {executor.submit(fetch_single_record, obj): obj for obj in objects}

            # Yield results as they complete
            for future in as_completed(future_to_obj):
                record = future.result()
                if record is not None:
                    yield record


def generate_caputure_objects_from_index(
    index_lines: List[str], warc_download_prefix=None, limit: int = 0, progress_bar: bool = False
) -> Iterable[cdx_toolkit.CaptureObject]:
    """Read CDX index and generate CaptureObject objects."""

    if limit > 0:
        index_lines = index_lines[:limit]

    # if progress_bar:
    #     index_lines = tqdm(index_lines, desc="Extracting from WARC", total=len(index_lines))

    for i, line in enumerate(index_lines, 1):
        cols = line.split(' ', maxsplit=2)

        if len(cols) == 3:
            # TODO can there be a different format?
            # surt, timestamp, json_data = cols
            #
            # CC seems to not follow the IIPC pecification
            # https://iipc.github.io/warc-specifications/specifications/cdx-format/cdx-2015/
            #
            # > The default first line of a CDX file is:
            # > CDX A b e a m s c k r V v D d g M n
            data = json.loads(cols[2])
            data['timestamp'] = cols[1]
        else:
            raise ValueError(f'Cannot parse line: {line}')

        yield cdx_toolkit.CaptureObject(data=data, wb=None, warc_download_prefix=warc_download_prefix)
