# cdx_toolkit

cdx_toolkit is a set of tools for working with CDX indices of web
crawls, including those at CommonCrawl and the Internet Archive's
Wayback Machine.

CommonCrawl uses Ilya Kramer's pywb to serve the CDX API, which is
somewhat different from the Internet Archive's CDX API. cdx_toolkit
hides these differences as best it can.

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
$ cdx_size.py 'commoncrawl.org/*' --cc
$ cdx_iter.py 'commoncrawl.org/*' --cc --limit 10 --cc-duration='90d'
```
or
```
$ cdx_size.py 'commoncrawl.org/*' --ia
$ cdx_iter.py 'commoncrawl.org/*' --ia --limit 10
```

cdx_iter can generate jsonl or csv outputs; see

```
$ cdx_iter.py --help
```

for details.

## Status

cdx_toolkit has reached the "I hacked this together out of some other
code for a hackathon this weekend" stage of development.

## License

Apache 2.0

