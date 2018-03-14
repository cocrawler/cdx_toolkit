#!/usr/bin/env python

import sys

import cdx_toolkit

source = sys.argv[1]
url = sys.argv[2]

cdx = cdx_toolkit.CDXFetcher(source=source)

for obj in cdx.items(url, limit=1):
    print(obj)
    pass

if source == 'cc':
    content_bytes = cdx_toolkit.fetch_warc_content(obj)
else:
    content_bytes = cdx_toolkit.fetch_wb_content(obj)

if len(content_bytes) > 100 and b'html' in content_bytes.lower():
    print('OK')
    exit(0)
else:
    print('FAIL')
    exit(1)
