from argparse import ArgumentParser
from collections import defaultdict
import json
import sys
import math
import glob

import s3fs
import pyarrow.parquet as pq

from progress.bar import Bar


def main(args=None):
    parser = ArgumentParser(description='parquet-stats command line tool')
    parser.add_argument('--anon', action='store_true', help='Use an anonymous connection, public buckets only')
    parser.add_argument('--requester-pays', action='store_true', help='use requester-pays access, will read S3 creds from env vars or files')
    parser.add_argument('--limit', action='append', help='limit analysis to this partition, e.g. date=2018-01-31')
    parser.add_argument('--aggregate', action='append', help='aggregate analysis across this partition, e.g. date')
    parser.add_argument('--overflow', action='store_true', help='try to analyze dictionary overflows')
    parser.add_argument('--overflow-print', action='store_true', help='print parquet filenames for overflowed dictionary columns')
    parser.add_argument('--pretty', action='store_true', help='pretty-print jsonl output')
    parser.add_argument('--dump-groups', action='store_true', help='just print the file groups and exit early')
    parser.add_argument('path', help='e.g. s3://commoncrawl/cc-index/table/cc-main/warc/*/*/*.parquet or **/*.parquet')

    cmd = parser.parse_args(args=args)
    print('cmd', cmd, file=sys.stderr)

    kwargs = {}
    if cmd.anon:
        kwargs['anon'] = True
    if cmd.requester_pays:
        kwargs['requester_pays'] = True  # defaults to False

    fs = None
    if cmd.path.startswith('s3://'):
        fs = s3fs.S3FileSystem(**kwargs)
        all_files = fs.glob(path=cmd.path, recursive=True)
    else:
        all_files = glob.glob(cmd.path, recursive=True)

    print('have', len(all_files), 'files', file=sys.stderr)

    all_files = apply_limit(all_files, cmd)
    print('have', len(all_files), 'files after limit applied', file=sys.stderr)

    file_groups = get_aggregate_groups(all_files, cmd)
    if file_groups:
        print('have', len(file_groups), 'file groups', file=sys.stderr)
    else:
        return

    if cmd.dump_groups:
        print(json.dumps(list(file_groups), sort_keys=True, indent=4))
        return

    results = do_work(file_groups, fs, cmd)
    print_result(results, pretty=cmd.pretty)


def apply_limit(all_files, cmd):
    if not cmd.limit:
        return all_files
    ret = []
    for f in all_files:
        if cmd.limit:
            for limit in cmd.limit:
                if limit not in f:
                    break
            else:
                ret.append(f)
    return ret


def get_aggregate_groups(all_files, cmd):
    if not all_files:
        return
    if not cmd.aggregate:
        return [(None, all_files)]

    ret = defaultdict(list)
    for f in all_files:
        labels = []
        for a in cmd.aggregate:
            parts = f.split('/')
            for p in parts:
                if p.startswith(a):
                    labels.append(p.replace(a, '', 1).replace('=', '', 1))
        if labels:
            # the order should be preserved
            key = ' '.join(labels)
            ret[key].append(f)
        else:
            # probably a bad sign, but...
            print('no label for', f, file=sys.stderr)

    return ret.items()


def check_same(thing1, thing2, what, fname):
    if thing1 is not None and thing1 != thing2:
        print('observed unusual value of {} for {} in {}'.format(what, thing2, fname))
    return thing2


def analyze_dictionary_overflow(column, statspath, fname, row_group_index, path, cmd):
    if 'hist_size' not in statspath:
        statspath['hist_size'] = defaultdict(int)

    try:
        bucket = int(math.log10(column.total_compressed_size))
        statspath['hist_size']['{:,}-{:,}'.format(10**bucket, 10**(bucket+1))] += 1
    except Exception as e:
        print('exception doing bucket math', e, file=sys.stderr)

    if 'overflow' not in statspath:
        statspath['overflow'] = defaultdict(int)

    if column.total_compressed_size < 100_000:
        statspath['overflow']['very small size'] += 1
        return

    if column.total_compressed_size < 1_000_000:
        if 'PLAIN_DICTIONARY' not in column.encodings:
            statspath['overflow']['small no dictionary'] += 1
            return

        if 'PLAIN' in column.encodings:
            statspath['overflow']['small dictionary overflowed to plain'] += 1
            if cmd.overflow_print:
                print('small overflow', fname, row_group_index, path, file=sys.stderr)
            return

        statspath['overflow']['small dictionary not overflowed'] += 1
        return

    if 'PLAIN_DICTIONARY' not in column.encodings:
        statspath['overflow']['big no dictionary'] += 1
        return

    if 'PLAIN' in column.encodings:
        statspath['overflow']['big dictionary overflowed to plain'] += 1
        if cmd.overflow_print:
            print('big overflow', fname, row_group_index, path, file=sys.stderr)
        return

    statspath['overflow']['big dictionary not overflowed'] += 1


def analyze_one(metadata, stats, num_columns, fname, cmd):
    stats['_created_by'][metadata.created_by] += 1
    num_columns = check_same(num_columns, metadata.num_columns, 'num_columns', fname)

    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        num_columns = check_same(num_columns, row_group.num_columns, 'num_columns', fname)
        stats['_row_groups'] += 1

        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            path = column.path_in_schema

            if path not in stats:
                stats[path] = defaultdict(int)

            if 'compression' not in stats[path]:
                stats[path]['compression'] = defaultdict(int)
            stats[path]['compression']['total_compressed_size'] += column.total_compressed_size
            stats[path]['compression']['total_uncompressed_size'] += column.total_uncompressed_size

            if 'physical_type' not in stats[path]:
                stats[path]['physical_type'] = defaultdict(int)
            stats[path]['physical_type'][column.physical_type] += 1

            encodings = ','.join(sorted(column.encodings))  # seems to be reordered by PyArrow so sort for stability
            if 'encodings' not in stats[path]:
                stats[path]['encodings'] = defaultdict(int)
            stats[path]['encodings'][encodings] += 1

            if column.is_stats_set:
                statistics = column.statistics
                if statistics.has_min_max:
                    stats[path]['min_max_present'] += 1
                else:
                    stats[path]['min_max_absent'] += 1
            else:
                stats[path]['min_max_no_stats_set'] += 1

            if cmd.overflow:
                analyze_dictionary_overflow(column, stats[path], fname, row_group_index, path, cmd)
    return num_columns


def my_smart_open(fname, fs):
    if fs:  # fname.startswith('s3://'):
        fp = fs.open(fname, mode='rb')
    else:
        fp = open(fname, mode='rb')
    return fp


def do_work(file_groups, fs, cmd):
    ret = []

    bar = Bar('files', max=sum([len(files) for group, files in file_groups]), suffix='%(index)d / %(max)d - %(percent).1f%%')

    for group, files in file_groups:
        num_columns = None
        stats = defaultdict(int)
        stats['_created_by'] = defaultdict(int)
        for fname in files:
            bar.next(1)
            try:
                fp = my_smart_open(fname, fs)
                pqf = pq.ParquetFile(fp)
                metadata = pqf.metadata
            except Exception as e:
                print(file=sys.stderr)
                print('exception {} processing file {}'.format(str(e), fname), file=sys.stderr)
                continue

            num_columns = analyze_one(metadata, stats, num_columns, fname, cmd)

        for path in stats:
            sp = stats[path]
            if isinstance(sp, int):
                continue
            if 'compression' in sp:
                spc = sp['compression']
                if 'total_compressed_size' in spc and 'total_uncompressed_size' in spc:
                    cr = spc['total_uncompressed_size']/spc['total_compressed_size']
                    if cr >= 10.0:
                        spc['total_compression_ratio'] = '{:.0f}:1'.format(int(cr))
                    else:
                        spc['total_compression_ratio'] = '{:.1f}:1'.format(cr)

        ret.append((group, stats))
    bar.finish()
    return ret


def print_result(results, pretty=False):
    if not results:
        return

    kwargs = {}
    if pretty:
        kwargs['indent'] = 4

    for group, stats in results:
        stats['_group'] = group
        print(json.dumps(stats, sort_keys=True, **kwargs))


if __name__ == '__main__':
    main()
