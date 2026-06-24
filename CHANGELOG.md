- unreleased (feat/warc-range-sources)
	+ new `repackage` command: extract a WARC subset from pluggable sources (cdx / sql{athena,duckdb} / csv), reading source WARCs over HTTP or S3 and writing verbatim
	+ `--processes N` multi-process fetching (one event loop per core); range jobs are sharded by WARC filename and the shards are merged into a single `<prefix>.warc.gz` with one warcinfo record (server-side on S3 / streamed locally). `--keep-shards` keeps the shards
	+ one writer per process (removed the multi-writer-per-process fan-out); `--parallel_readers` sets async readers per process
	+ optional uvloop event loop via `CDXT_UVLOOP=1`
	+ read source WARCs from a Hugging Face Storage Bucket: `--warc-download-prefix=hf://buckets/<ns>/<name>` with `--hf-reader fsspec` (HfFileSystem) or `--hf-reader cdn` (async ranged GETs against the CDN resolve URL); repackage output may also be written to an `hf://` bucket (shards staged locally, merged via fsspec). Needs the `hf` extra (`pip install cdx_toolkit[hf]`)
	+ see docs/notes/warc-fetcher-performance.md for benchmarks and tuning

- 0.9.38
	+ deprecated support for py3.7 and py.3.8
	+ added support for py3.13 and py3.14
	+ fixed deprecation warning from pkg_resources
	+ refactored CI to use mocked remote requests

- 0.9.37
	+ --crawl for CCF

- 0.9.36
	+ ratelimit code; both IA and CCF are rate limiting their cdx endpoints
	+ cache collinfo.json in ~/.cache/cdx_toolkit/
	+ py3.11 and py3.12 pass testing; windows and macos pass testing

- 0.9.35
    + exponential backoff retries now that IA is sending 429

- 0.9.34
	+ renamed class CDXFetcher kwarg warc_url_prefix to warc_download_prefix
	+ changed CC warc downloads to use the Cloudfront url, which has rate limits
	+ made 429 and 509 retryable errors. Neither IA or CC sends this status.

- 0.9.33
	+ rename master to main
	+ drop python 3.5 testing because of setuptools-scm

- 0.9.32
	+ there was no 0.9.32

- 0.9.31
	+ pywb 2.5 changed a json error message key
	+ tolerate the first capture returned for example.com being a revisit record

- 0.9.30
	+ add support for CC early indices, 2008-2010
	+ MacOS support marked in setup.py

- 0.9.29
	+ python 3.9 support marked in setup.py

- 0.9.28
	+ expose warc_version= keyword argument for warc writing (but it's untested and broken for --ia warc)
	+ improve dns retry algorithm: always retry for hostnames we expect to exist (ia, cc)

- 0.9.27
	+ packaging: fix for using markdown without explicit conversion in setup.py

- 0.9.26
	+ 10x effort for cdx server timeouts, but fail immediately for dns failures
	+ give a human-useful error if the user passes in a unix timestamp instead of a cdx timestamp

- 0.9.25
	+ allow multiple --filter args in the cli, and filter=list in the api
	+ drop python 3.4 support because requests 2.22 no longer supports it
	+ python 3.8 works

- 0.9.24
	+ rename cdx.items() to cdx.iter() with a deprecation warning
	+ use warcio for all warc reading/writing
	+ test coverage 99%

- 0.9.23
	+ add 'cdxt' command-line tool
	+ deprecate cdx_iter and cdx_size command-line scripts
	+ migrate tests to use 'cdxt' with much better error-checking
	+ add warc 'subprefix' to warcinfo isPartOf line
	+ made default limit=1000 apply only to get, not iter
	+ make iterator results be delivered incrementally
	+ start changelog

