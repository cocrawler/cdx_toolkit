#!/usr/bin/env python

import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='cc')
url = 'commoncrawl.org/*'

warcinfo = {
    'software': 'pypi_cdx_toolkit iter-and-warc example',
    'isPartOf': 'EXAMPLE-COMMONCRAWL',
    'description': 'warc extraction',
    'format': 'WARC file version 1.0',
}

writer = cdx_toolkit.warc.get_writer('EXAMPLE', 'COMMONCRAWL', warcinfo, warc_version='1.1')

for obj in cdx.iter(url, limit=10):
    url = obj['url']
    status = obj['status']
    timestamp = obj['timestamp']

    print('considering extracting url', url, 'timestamp', timestamp)
    if status != '200':
        print(' skipping because status was {}, not 200'.format(status))
        continue

    try:
        record = obj.fetch_warc_record()
    except RuntimeError:
        print(' skipping capture for RuntimeError 404: %s %s', url, timestamp)
        continue
    writer.write_record(record)

    print(' wrote', url)
