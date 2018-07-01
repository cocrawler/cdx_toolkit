'''
Code that irons out the differences between IA and CommonCrawl/pywb
'''
import re

fields_to_cc = {'statuscode': 'status', 'original': 'url', 'mimetype': 'mime'}
fields_to_ia = dict([(v, k) for k, v in fields_to_cc.items()])


def munge_filter(filter, source):
    if source == 'ia':
        for bad in ('=', '!=',  '~',  '!~'):
            if filter.startswith(bad):
                raise ValueError('ia does not support the filter '+bad)
        for k, v in fields_to_ia.items():
            filter = re.sub(r'\b'+k+':', v+':', filter, 1)
    if source == 'cc':
        for k, v in fields_to_cc.items():
            filter = re.sub(r'\b'+k+':', v+':', filter, 1)
    return filter


def munge_fields(fields, lines):
    ret = []
    for l in lines:
        obj = {}
        for f in fields:
            value = l.pop(0)
            if f in fields_to_cc:
                obj[fields_to_cc[f]] = value
            else:
                obj[f] = value
        ret.append(obj)
    return ret
