# cdx_toolkit

[![Build Status](https://travis-ci.org/cocrawler/cdx_toolkit.svg?branch=master)](https://travis-ci.org/cocrawler/cdx_toolkit) [![Coverage Status](https://coveralls.io/repos/github/cocrawler/cdx_toolkit/badge.svg?branch=master)](https://coveralls.io/github/cocrawler/cdx_toolkit?branch=master) [![Apache License 2.0](https://img.shields.io/github/license/cocrawler/cdx_toolkit.svg)](LICENSE)

cdx_toolkit is a set of tools for working with CDX indices of web
crawls and archives, including those at CommonCrawl and the Internet
Archive's Wayback Machine.

CommonCrawl uses Ilya Kramer's pywb to serve the CDX API, which is
somewhat different from the Internet Archive's CDX API. cdx_toolkit
hides these differences as best it can. cdx_toolkit also knits
together the monthly Common Crawl CDX indices into a single, virtual
index.

## Installing

cdx toolkit requires Python 3.

```
$ pip install cdx_toolkit
```

or clone this repo and use `python setup.py install`.

## Example

```
import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='cc')
url = 'commoncrawl.org/*'

print(url, 'size estimate', cdx.get_size_estimate(url))

for obj in cdx.items(url, limit=10):
    print(obj)
```

at the moment will print:

```
commoncrawl.org/* size estimate 36000
{'urlkey': 'org,commoncrawl)/', 'timestamp': '20180219112308', 'mime-detected': 'text/html', 'url': 'http://commoncrawl.org/', 'status': '200', 'filename': 'crawl-data/CC-MAIN-2018-09/segments/1518891812584.40/warc/CC-MAIN-20180219111908-20180219131908-00494.warc.gz', 'mime': 'text/html', 'length': '5365', 'digest': 'FM7M2JDBADOQIHKCSFKVTAML4FL2HPHT', 'offset': '81614902'}
...
```

## Command-line tools

```
$ cdx_size 'commoncrawl.org/*' --cc
$ cdx_iter 'commoncrawl.org/*' --cc --limit 10
```

```
$ cdx_size 'commoncrawl.org/*' --ia
$ cdx_iter 'commoncrawl.org/*' --ia --limit 10
```

cdx_iter takes a large number of command line switches, controlling
the time period and all other CDX query options.
It can generate jsonl and csv outputs.  See

```
$ cdx_iter --help
```

for details. Set the environment variable LOGLEVEL=DEBUG if you'd like
more details about what's going on inside cdx_iter.

## CDX Jargon, Field Names, and such

cdx_toolkit supports all of the options and fields discussed
in the CDX API documentation:

* https://github.com/webrecorder/pywb/wiki/CDX-Server-API
* https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server

A **capture** is a single crawled url, be it a copy of a webpage, a
redirect to another page, an error such as 404 (page not found), or a
revisit record (page identical to a previous capture.)

The **url** used by cdx_tools can be wildcarded in two ways. One way
is `*.example.com`, which in CDX jargon sets **matchType='domain'**, and
will return captures for blog.example.com, support.example.com, etc.
The other, `example.com/*`, will return captures for any page on
example.com.

A **timestmap** represents year-month-day-time as a string of digits run togther.
Example: January 5, 2016 at 12:34:56 UTC is 20160105123456. These timestamps are
a field in the index, and are also used to pick specify the dates used
by **--from=**, **--to**, and **--closest** on the command-line. (Programmatically,
use **from_ts=**, to=, and closest=.)

An **urlkey** is a SURT, which is a munged-up url suitable for
deduplication and sorting. This sort order is how CDX indices
efficiently support queries like `*.example.com`. The SURTs for
www.example.com and example.com are identical, which is handy when
these 2 hosts actually have identical web content. The original url
should be present in all records, if you want to know exactly what it
is.

The **limit** argument limits how many captures will be returned.
**There is a default limit of 1,000 captures.**

A **filter** allows a user to select a subset of CDX records, reducing
network traffic between the CDX API server and the user. For
example, filter='!=status:200' will only show captures whose http
status is not 200. Filters and **limit** work together, with the limit
applying to the count of captures after the filter is applied.

CDX API servers support a **paged interface** for efficient access to
large sets of URLs. cdx_toolkit iterators always use the paged interface.
cdx_toolkit is also polite to CDX servers by being single-threaded and
serial. If it's not fast enough for you, consider downloading Common
Crawl's index files directly.

A **digest** is a sha1 checksum of the contents of a capture. The
purpose of a digest is to be able to easily figure out if 2 captures
have identical content.

Common Crawl publishes a new index each month. cdx_toolkit will
start using new ones as soon as they are published. **By default,
cdx_toolkit will use the 12 mnoths of Common Crawl**; you can
change that using **--from** or **from=** and **--to** or **to=**.

CDX implementations do not efficiently support reversed sort orders,
so cdx_toolkit results will be ordered by ascending SURT and by
ascending timestamp. However, since CC has an individual index for
each month, and because most users want more recent results,
cdx_toolkit defaults to querying CC's CDX indices in decreasing month
order, but each month's result will be in ascending SURT and ascending
timestamp. If you'd like pure ascending, set **--cc-sort** or
**cc_sort=** to 'ascending'. You may want to also specify **--from**
or **from_ts=** to set a starting timestamp.

The main problem with this ascending sort order is that it's a pain to
get the most recent N captures: --limit and limit= will return the
oldest N captures.

## TODO

Add a call to download a capture from ia or cc, given an URL and a timestamp.

## Status

cdx_toolkit has reached the beta-testing stage of development.

## License

Apache 2.0

