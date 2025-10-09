import fsspec
import pytest
from cdx_toolkit.filter_warc.cdx_utils import get_index_as_string_from_path, read_cdx_line, iter_cdx_index_from_path
from tests.conftest import TEST_DATA_PATH

import tempfile
import gzip
import os
from unittest.mock import patch



def test_iter_cdx_index_from_test_data():
    cdx_path = TEST_DATA_PATH / 'warc_by_cdx/filtered_CC-MAIN-2024-30_cdx-00187.gz'
    results = list(iter_cdx_index_from_path(str(cdx_path), 'http://warc-prefix'))
    # [(url, offset, length)]

    # sort results by offsets
    results.sort(key=lambda x: x[1])

    # group into neighbor chunks
    def group_neighbor_chunks(items):
        """Group items into chunks where items have same URL and are contiguous."""
        if not items:
            return []

        chunks = []
        current_chunk = [items[0]]

        for i in range(1, len(items)):
            prev_url, prev_offset, prev_length = items[i-1]
            curr_url, curr_offset, curr_length = items[i]

            # Check if current item is a neighbor (same URL and contiguous)
            if curr_url == prev_url and curr_offset == prev_offset + prev_length + 4:
                current_chunk.append(items[i])
            else:
                # Start new chunk
                chunks.append(current_chunk)
                current_chunk = [items[i]]

        # Add the last chunk
        chunks.append(current_chunk)
        return chunks

    grouped_chunks = group_neighbor_chunks(results)
    print(len(results), len(grouped_chunks))


def test_grouped_ranges():
    cdx_path = ""