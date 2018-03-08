#!/bin/sh

# Clue: LOGLEVEL=DEBUG environment variable

# make sure we exit immediately when there's an error, for CI:
set -e

# if COVERAGE is set, use it, else python
if [ -z "$COVERAGE" ]; then COVERAGE=python; fi

# these are all "does it crash" tests, mostly

$COVERAGE ../scripts/cdx_size 'commoncrawl.org/*' --cc
echo limit 10
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --cc --limit 10
echo limit 20
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org' --cc --limit 20
echo to 2017
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org' --cc --limit 10 --to=2017
echo from 2017 to 2017
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org' --cc --limit 10 --from=2017 --to=2017
echo filter
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org' --cc --limit 10 --from=2017 --to=2017 --filter=!=status:200
echo closest cc
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --cc --get --closest=2017 --limit 3
echo csv
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org' --cc --limit 10 --csv
echo jsonl
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org' --cc --limit 10 --jsonl

$COVERAGE ../scripts/cdx_size 'commoncrawl.org/*' --ia
echo limit 10
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --ia --limit 10
echo closest
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --ia --get --closest=2017 --limit 10

echo alternate source
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --source https://web.archive.org/cdx/search/cdx --limit 10
echo bad alternate source
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --source https://web.arch457374576ive.org/cdx/search/cdx --limit 10 || true
