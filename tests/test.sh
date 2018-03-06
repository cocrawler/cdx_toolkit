#!/bin/sh

# Clue: LOGLEVEL=DEBUG environment variable

# make sure we exit immediately when there's an error, for CI:
set -e

# if COVERAGE is set, use it, else python
if [ -z "$COVERAGE" ]; then COVERAGE=python; fi

# these next 4 are from README.md -- we aren't checking the answer, but at least we know they didn't crash
$COVERAGE ../scripts/cdx_size 'commoncrawl.org/*' --cc
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --cc --limit 10 --cc-duration '90d'

$COVERAGE ../scripts/cdx_size 'commoncrawl.org/*' --ia
$COVERAGE ../scripts/cdx_iter 'commoncrawl.org/*' --ia --limit 10
