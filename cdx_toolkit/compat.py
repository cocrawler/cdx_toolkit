'''
Code that irons out the differences between IA/java wayback and pywb
'''
import re

fields_to_pywb = {'statuscode': 'status', 'original': 'url', 'mimetype': 'mime'}
fields_to_ia = dict([(v, k) for k, v in fields_to_pywb.items()])


def munge_filter(filter, source):
    ret = []
    for f in filter:
        if source == 'ia':
            for bad in ('=', '!=',  '~',  '!~'):
                if f.startswith(bad):
                    raise ValueError('ia does not support the filter '+bad)
            for k, v in fields_to_ia.items():
                f = re.sub(r'\b'+k+':', v+':', f, 1)
        else:  # assume cc or other are both pywb
            for k, v in fields_to_pywb.items():
                f = re.sub(r'\b'+k+':', v+':', f, 1)
        # other sources (e.g. source=url-of-a-wayback) are not transformed
        ret.append(f)
    return ret


def munge_fields(fields, lines):
    ret = []
    for l in lines:
        obj = {}
        for f in fields:
            value = l.pop(0)
            if f in fields_to_pywb:
                obj[fields_to_pywb[f]] = value
            else:
                obj[f] = value
        ret.append(obj)
    return ret
