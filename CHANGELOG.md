- 0.9.34 (not yet tagged)
	+ experimental support for CC's columnar index

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

