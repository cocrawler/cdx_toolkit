from cdx_toolkit.filter_warc.cdx_utils import get_cdx_paths
from cdx_toolkit.filter_warc.warc_filter import WARCFilter
from cdx_toolkit.utils import get_version, setup


import fsspec


import time
import logging

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
    logger.info('Filtering WARC files based on CDX')

    # Start timing
    start_time = time.time()

    write_paths_as_resource_records = args.write_paths_as_resource_records
    write_paths_as_resource_records_metadata = args.write_paths_as_resource_records_metadata

    if write_paths_as_resource_records and write_paths_as_resource_records_metadata:
        if len(write_paths_as_resource_records) != len(write_paths_as_resource_records_metadata):
            raise ValueError('Number of paths to resource records must be equal to metadata paths.')

    if not write_paths_as_resource_records and write_paths_as_resource_records_metadata:
        raise ValueError('Metadata paths are set but resource records paths are missing.')

    if args.is_part_of:
        ispartof = args.is_part_of
    else:
        ispartof = args.prefix
        if args.subprefix:
            ispartof += '-' + args.subprefix

    info = {
        'software': 'pypi_cdx_toolkit/' + get_version(),
        'isPartOf': ispartof,
        'description': args.description
        if args.description
        else 'warc extraction based on CDX generated with: ' + cmdline,
        'format': 'WARC file version 1.0',
    }
    if args.creator:
        info['creator'] = args.creator
    if args.operator:
        info['operator'] = args.operator

    # writer_kwargs = {}
    # if 'size' in kwargs:
    #     writer_kwargs['size'] = kwargs['size']
    #     del kwargs['size']

    n_parallel = args.parallel
    log_every_n = args.log_every_n
    limit = 0 if args.limit is None else args.limit
    prefix_path = str(args.prefix)
    prefix_fs, prefix_fs_path = fsspec.url_to_fs(prefix_path)

    # make sure the base dir exists
    prefix_fs.makedirs(prefix_fs._parent(prefix_fs_path), exist_ok=True)
    
    # target source handling
    cdx_paths = None
    athena_where_clause = None

    if args.target_source == 'cdx':
        cdx_paths = get_cdx_paths(
            args.cdx_path,
            args.cdx_glob,
        )
    elif args.target_source == "athena":
        raise NotImplementedError
    else:
        raise ValueError(f'Invalid target source specified: {args.target_source} (available: cdx, athena)')

    warc_filter = WARCFilter(
        cdx_paths=cdx_paths,
        athena_where_clause=athena_where_clause,
        prefix_path=prefix_path,
        writer_info=info,
        writer_subprefix=args.subprefix,
        write_paths_as_resource_records=write_paths_as_resource_records,
        write_paths_as_resource_records_metadata=write_paths_as_resource_records_metadata,
        record_limit=limit,
        log_every_n=log_every_n,
        warc_download_prefix=args.warc_download_prefix,
        n_parallel=n_parallel,
        max_file_size=args.size,
        # writer_kwargs=writer_kwargs,
    )
    records_n = warc_filter.filter()

    logger.info('WARC records extracted: %i', records_n)

    # End timing and log execution time
    end_time = time.time()
    execution_time = end_time - start_time

    logger.info(f'Script execution time: {execution_time:.3f} seconds')
