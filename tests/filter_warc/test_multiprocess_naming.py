"""Unit tests for multi-process output naming / shard-file collection.

Pure path logic, no S3 or fetching. Covers the global renumber series naming and that
a worker's rotated shard files are discovered and sorted by rotation sequence.
"""
import os

from cdx_toolkit.filter_warc.multiprocess import _output_filename, _collect_shard_files


def test_output_filename_local_global_series():
    # No subprefix -> the clean global series matching single-process naming.
    assert _output_filename('/out/run', 1) == '/out/run-001.warc.gz'
    assert _output_filename('/out/run', 12) == '/out/run-012.warc.gz'


def test_output_filename_s3():
    assert _output_filename('s3://b/p/run', 3) == 's3://b/p/run-003.warc.gz'


def test_collect_shard_files_local_sorted_by_sequence(tmp_path):
    base = os.path.join(str(tmp_path), 'run')
    # Two shards, each rotated into several files; create out of order.
    made = []
    for sub, seqs in (('sh00', [3, 1, 2]), ('sh01', [1, 2])):
        for s in seqs:
            p = f'{base}-{sub}-{s:03d}.warc.gz'
            open(p, 'w').close()
            made.append(p)
    # An unrelated file that must NOT be collected for sh00.
    open(f'{base}-sh00x-001.warc.gz', 'w').close()

    got = _collect_shard_files(base, 'sh00', aws_region_name='us-east-1')
    assert got == [f'{base}-sh00-001.warc.gz', f'{base}-sh00-002.warc.gz', f'{base}-sh00-003.warc.gz']

    got1 = _collect_shard_files(base, 'sh01', aws_region_name='us-east-1')
    assert got1 == [f'{base}-sh01-001.warc.gz', f'{base}-sh01-002.warc.gz']


def test_collect_shard_files_empty(tmp_path):
    base = os.path.join(str(tmp_path), 'run')
    assert _collect_shard_files(base, 'sh07', aws_region_name='us-east-1') == []
