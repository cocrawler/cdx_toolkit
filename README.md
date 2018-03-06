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

https://github.com/webrecorder/pywb/wiki/CDX-Server-API
https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server

## Example

```
import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='cc', cc_duration='90d')
url = 'commoncrawl.org/*'

print(url, 'size estimate', cdx.get_size_estimate(url))

for obj in cdx.items(url, limit=10):
    print(obj)
```

at the moment will print:

```
size estimate 6000
http://commoncrawl.org/ 200
http://commoncrawl.org/ 200
http://commoncrawl.org/ 200
http://www.commoncrawl.org/ 301
https://www.commoncrawl.org/ 301
http://www.commoncrawl.org/ 301
http://commoncrawl.org/ 200
http://commoncrawl.org/2011/12/mapreduce-for-the-masses/ 200
http://commoncrawl.org/2012/03/data-2-0-summit/ 200
http://commoncrawl.org/2012/03/twelve-steps-to-running-your-ruby-code-across-five-billion-web-pages/ 200
```

## Command-line tools

The above example can also be done as

```
$ cdx_size 'commoncrawl.org/*' --cc
$ cdx_iter 'commoncrawl.org/*' --cc --limit 10 --cc-duration='90d'
```
or
```
$ cdx_size 'commoncrawl.org/*' --ia
$ cdx_iter 'commoncrawl.org/*' --ia --limit 10
```

cdx_iter can generate jsonl or csv outputs; see

```
$ cdx_iter --help
```

for details.

## CDX Jargon, Field Names, and such

A **capture** is a single crawled url, be it a copy of a webpage, a
redirect to another page, an error such as 404 (page not found), or a
revisit record (page identical to a previous capture.)

The **url** used by cdx_tools can be wildcarded in two ways. One way
is '*.example.com', which in CDX jargon sets **matchType='domain'**, and
will return captures for blog.example.com, support.example.com, etc.
The other, 'example.com/*', will return captures for any page on
example.com.

A **timestmap** represents year-month-day-time as a string of digits run togther.
Example: January 5, 2016 at 12:34:56 UTC is 20160105123456. These timestamps are
a field in the index, and are also used to pick specify the dates used
by **--from=**, **--to**, and **--closest** on the command-line. (Programmatically,
use from_ts=, to=, and closest=.)

An **urlkey** is a SURT, which is a munged-up url suitable for
deduplication and sorting. This sort order is how CDX indices
efficiently support queries like *.example.com. The SURTs for
www.example.com and example.com are identical, which is handy when
these 2 hosts actually have identical web content. The original url
should be present in all records, if you want to know exactly what it
is.

CDX Indices support a **paged interface** for efficient access to
large sets of URLs. cdx_toolkit uses this interface under the hood.
cdx_toolkit is also polite to CDX servers by being single-threaded and
serial. If it's not fast enough for you, consider downloading Common
Crawl's index files directly.

A **digest** is a sha1 checksum of the contents of a capture. The
purpose of a digest is to be able to easily figure out if 2 captures
have identical content.

Common Crawl publishes a new index each month. cdx_toolkit will
automatically start using new ones as published. The **--cc-duration**
command-line flag (and **cc_duration=** constructor argument)
specifies how many days back to look. The default is '365d', 365
days.

CDX implementations do not efficiently support reversed sort orders,
so cdx_toolkit results will be ordered by ascending SURT and by
ascending timestamp. However, since CC has an individual index for
each month, and because most users want more recent results,
cdx_toolkit defaults to querying CC's CDX indices in decreasing month
order, but each month's result will be in ascending SURT and ascending
timestamp. If you'd like pure ascending, set **--cc-sort** or
**cc_sort=** to 'ascending'. You may want to also specify **--from**
or **from_ts=** to set a starting timestamp.

The main problem with this ascending sort order is that it's a pain
to get the most recent N captures for something.

## TODO

Add a call to download a capture from ia or cc, given an URL and a timestamp.

Unit tests / CI / coverage

End-to-end tests

## Status

cdx_toolkit has reached the "I hacked this together out of some other
code for a hackathon this weekend" stage of development.

## License

Apache 2.0

