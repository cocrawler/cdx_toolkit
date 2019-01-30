# cdx_toolkit

[![Build Status](https://travis-ci.org/cocrawler/cdx_toolkit.svg?branch=master)](https://travis-ci.org/cocrawler/cdx_toolkit) [![Coverage Status](https://coveralls.io/repos/github/cocrawler/cdx_toolkit/badge.svg?branch=master)](https://coveralls.io/github/cocrawler/cdx_toolkit?branch=master) [![Apache License 2.0](https://img.shields.io/github/license/cocrawler/cdx_toolkit.svg)](LICENSE)

cdx_toolkit is a set of tools for working with CDX indices of web
crawls and archives, including those at CommonCrawl and the Internet
Archive's Wayback Machine.

CommonCrawl uses Ilya Kreymer's pywb to serve the CDX API, which is
somewhat different from the Internet Archive's CDX API server. cdx_toolkit
hides these differences as best it can. cdx_toolkit also knits
together the monthly Common Crawl CDX indices into a single, virtual
index.

Finally, cdx_toolkit allows extracting archived pages from CC
and IA into WARC files. This feature is a work-in-progress and
still has several warts.

## Installing

cdx toolkit requires Python 3.

```
$ pip install cdx_toolkit
```

or clone this repo and use `python ./setup.py install`.

## Example

```
import cdx_toolkit

cdx = cdx_toolkit.CDXFetcher(source='cc')
url = 'commoncrawl.org/*'

print(url, 'size estimate', cdx.get_size_estimate(url))

for obj in cdx.iter(url, limit=1):
    print(obj)
```

at the moment will print:

```
commoncrawl.org/* size estimate 36000
{'urlkey': 'org,commoncrawl)/', 'timestamp': '20180219112308', 'mime-detected': 'text/html', 'url': 'http://commoncrawl.org/', 'status': '200', 'filename': 'crawl-data/CC-MAIN-2018-09/segments/1518891812584.40/warc/CC-MAIN-20180219111908-20180219131908-00494.warc.gz', 'mime': 'text/html', 'length': '5365', 'digest': 'FM7M2JDBADOQIHKCSFKVTAML4FL2HPHT', 'offset': '81614902'}
```

You can also fetch the content as bytes:

```
    print(obj.content)
```

## Command-line tools

```
$ cdxt --cc size 'commoncrawl.org/*'
$ cdxt --cc --limit 10 iter 'commoncrawl.org/*'
$ cdxt --cc --limit 10 --filter '=status:200' iter 'commoncrawl.org/*'
$ cdxt --ia --limit 10 iter 'commoncrawl.org/*'
```

cdxt takes a large number of command line switches, controlling
the time period and all other CDX query options. cdxt can generate
WARC, jsonl, and csv outputs.

** Note that by default, cdxt --cc will iterate over the previous
year of captures. **

See

```
$ cdxt --help
$ cdxt iter --help
$ cdxt warc --help
```

for full details. Note that argument order really matters; each switch
is valid only either before or after the {iter,warc,size} command.

Add -v (or -vv) to see what's going on under the hood.

## Filter syntax

Filters can be used to limit captures to a subset of the results.

Any field name listed in `cdxt iter --all-fields` can be used in a
filter.  These field names are appropriately renamed if the source is
'ia'.  The different syntax of filter modifiers for 'ia' and 'cc' is
not fully abstracted away by cdx_toolkit.

The basic syntax of a filter is `[modifier]field:expression`, for
example `=status:200` or `!=status:200`.

'cc'-style filters (pywb) come in six flavors: substring match, exact
string, full-match regex, and their inversions. These are indicated by
a modifier of nothing, '=', '\~', '!', '!=', and '!\~', respectively.

'ia'-style filters (Wayback/OpenWayback) come in two flavors, a full-match
regex and an inverted full-match regex: 'status:200' and '!status:200'

Multiple filters will be combined with AND. For example, to limit
captures to those which do not have status 200 and do not have status 404,

```
$ cdxt --cc --filter '!=status:200' --filter '!=status:404' iter ...
```

Note that filters that discard large numbers of captures put a high
load on the CDX server -- for example, a filter that returns just a
few captures from a domain that has tens of millions of captures is
likely to run very slowly and annoy the owner of the CDX server.

See https://github.com/webrecorder/pywb/wiki/CDX-Server-API#filter (pywb)
and https://github.com/internetarchive/wayback/tree/master/wayback-cdx-server#filtering (wayback)
for full details of filter modifiers.

## CDX Jargon, Field Names, and such

cdx_toolkit supports all (ok, most!) of the options and fields discussed
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

The **limit** argument limits how many captures will be returned.  To
help users not shoot themselves in the foot, a limit of 1,000 is
applied to --get and .get() calls.

A **filter** allows a user to select a subset of CDX records, reducing
network traffic between the CDX API server and the user. For example,
filter='!status:200' will only show captures whose http status is not
200. Multiple filters can be specified as a list (in the api) and on
the command line (by specifying --filter more than once). Filters and
**limit** work together, with the limit applying to the count of
captures after the filter is applied. Note that revisit records have a
status of '-', not 200.

CDX API servers support a **paged interface** for efficient access to
large sets of URLs. cdx_toolkit iterators always use the paged interface.
cdx_toolkit is also polite to CDX servers by being single-threaded and
serial. If it's not fast enough for you, consider downloading Common
Crawl's index files directly.

A **digest** is a sha1 checksum of the contents of a capture. The
purpose of a digest is to be able to easily figure out if 2 captures
have identical content.

Common Crawl publishes a new index each month. cdx_toolkit will start
using new ones as soon as they are published. **By default,
cdx_toolkit will use the most recent 12 months of Common Crawl**; you
can change that using **--from** or **from_ts=** and **--to** or
**to=**.

CDX implementations do not efficiently support reversed sort orders,
so cdx_toolkit results will be ordered by ascending SURT and by
ascending timestamp. However, since CC has an individual index for
each month, and because most users want more recent results,
cdx_toolkit defaults to querying CC's CDX indices in decreasing month
order, but each month's result will be in ascending SURT and ascending
timestamp. This default sort order is named 'mixed'. If you'd like
pure ascending, set **--cc-sort** or **cc_sort=** to 'ascending'. You
may want to also specify **--from** or **from_ts=** to set a starting
timestamp.

The main problem with this ascending sort order is that it's a pain to
get the most recent N captures: --limit and limit= will return the
oldest N captures. With the 'mixed' ordering, a large enough limit=
will get close to returning the most recent N captures.

## TODO

Content downloading needs help with charset issues, preferably
figuring out the charset using an algorithm similar to browsers.

WARC generation should do smart(er) things with revisit records.

Right now the CC code selects which monthly CC indices to use based
solely on date ranges. It would be nice to have an alternative so that
a client could iterate against the most recent N CC indices, and
also have the default one-year lookback use an entire monthly index
instead of a partial one.

## Status

cdx_toolkit has reached the beta-testing stage of development.

## License

Apache 2.0

