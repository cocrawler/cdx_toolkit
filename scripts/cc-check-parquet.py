#!/usr/bin/env python

import sys
from collections import defaultdict
import json

import jsonlines

'''
Criteria for a good CommonCrawl crawl

'parquet-mr version ' > 1.8.1
physical_type has exactly one entry

all url_host_* fields have min and max
  plus url_path, url_query, url_surtkey -- is this url_* ?
    ok url_protocol and url_port are the only ones not on this list
    url_port is very small and is lacking a lot of min and max

all fields, small dictionary overflows are rare (?)

'''


def analyze_parquet_version(created_by, complaints):
    parts = created_by.split(' ')
    assert parts[0] == 'parquet-mr'
    assert parts[1] == 'version'
    semver = parts[2].split('.')
    if semver[0] == '1' and int(semver[1]) < 10:
        complaints.append('parquet version too old: '+parts[2])


def analyze(obj):
    complaints = []
    group = obj['_group']
    created_by = obj['_created_by'].keys()
    for cb in created_by:
        analyze_parquet_version(cb, complaints)

    for k, v in obj.items():
        if k.startswith('_'):
            continue
        if 'physical_type' not in v:
            print('no physical type', k)
        if len(v['physical_type']) != 1:
            complaints.append('multiple phsyical_types in field: '+k)
        if 'small dictionary overflowed to plain' in v['overflow']:
            small = 0
            for k2, v2 in v['overflow'].items():
                if k2.startswith('small '):
                    small += v2
            ratio = v['overflow']['small dictionary overflowed to plain'] / small
            if ratio > 0.2:
                complaints.append('field has {:.1f}% small dictionary overflows: {}'.format(ratio*100, k))
        if k.startswith('url_'):
            if 'min_max_absent' in v or 'min_max_no_stats_set' in v:
                present = v.get('min_max_present', 0)
                absent = v.get('min_max_absent', 0) + v.get('min_max_no_stats_set', 0)
                percent = absent / (present + absent) * 100.
                complaints.append('field is missing stats {:4.1f}% of the time: {}'.format(percent, k))

    return group, complaints


overall = {}
with jsonlines.open(sys.argv[1]) as reader:
    for obj in reader.iter(type=dict, skip_invalid=True):
        group, complaints = analyze(obj)
        overall[group] = list(sorted(complaints))

print(json.dumps(overall, sort_keys=True, indent=4))
