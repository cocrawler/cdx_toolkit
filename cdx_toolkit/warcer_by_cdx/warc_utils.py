from io import BytesIO
import json
from pathlib import Path
import fsspec
from warcio.recordloader import ArcWarcRecord
from warcio import WARCWriter

from typing import Optional, Union

import mimetypes

def get_bytes_from_warc_record(
    record, 
    warc_version: str = '1.0',
    gzip: bool = False,
    ):
    buffer = BytesIO()
    warc_writer = WARCWriter(buffer, gzip=gzip, warc_version=warc_version)
    warc_writer.write_record(record)

    return buffer.getvalue()

def get_resource_record_from_path(
    file_path: Union[str, Path],
    metadata_path: Optional[Union[str, Path]] = None,
    ) -> ArcWarcRecord:
    """Build WARC resource record for file path and metdata path.
    
    The metadata file must be a valid JSON and can have the following fields:
    - warc_content_type
    - uri
    - http_headers
    - warc_headers_dict

    If uri is not provided as metadata, the file_path is used.
    If warc_content_type is not provided as metadata, the type is guessed.
    """
    # Cast to string
    file_path = str(file_path)
    
    with fsspec.open(file_path, "rb") as f:
        file_bytes = BytesIO(f.read())

    if metadata_path:
        # Load metadata from path
        metadata_path = str(metadata_path)

        if not metadata_path.endswith(".json"):
            raise ValueError("Metadata must be provided JSON (file path ends with *.json)")
        
        with fsspec.open(metadata_path) as f:
            metadata = json.load(f)

            warc_content_type = metadata.get("warc_content_type", None)
            uri = metadata.get("uri", None)
            http_headers = metadata.get("http_headers", None)
            warc_headers_dict = metadata.get("warc_headers_dict", None)
    else:
        # Without metdata
        warc_content_type = None
        uri = None
        http_headers = None
        warc_headers_dict = None

    if warc_content_type is None:
        warc_content_type = mimetypes.guess_type(file_path)[0]

    if uri is None:
        uri = file_path

    return WARCWriter(None).create_warc_record(
        uri=uri,
        record_type='resource',
        payload=file_bytes,
        http_headers=http_headers,
        warc_content_type=warc_content_type,
        warc_headers_dict=warc_headers_dict,
    )