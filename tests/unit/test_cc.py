import unittest.mock as mock
import pytest

import cdx_toolkit.commoncrawl
import cdx_toolkit.timeutils

# useful for debugging:
import logging
logging.basicConfig(level='INFO')


def test_apply_cc_defaults():
    # no from
    #  closest -- sets from, to
    #  to -- sets from
    #  else -- sets from and not to

    # at this point, we're guaranteed from is set

    # no to
    #  closest -- there was from AND closest and no to, now I set to
    #  else -- no to set at all, ok

    now = 1524962339.157388  # 20180429003859

    tests = [
        [{'closest': '20180101'}, {'from_ts': '20171003000000', 'to': '20180401000000'}],
        [{'closest': '20180101', 'to': '20181201'}, {'from_ts': '20171003000000'}],
        [{'to': '20180101'}, {'from_ts': '20170131235959'}],
        [{}, {'from_ts': '20170429003859'}],  # hits both elses, uses now
        [{'from_ts': '20100101', 'closest': '20150301'}, {'to': '20150530000000'}],
        [{'from_ts': '20100101'}, {}],  # hits the second else only
    ]

    for test_in, test_out in tests:
        test_out.update(test_in)
        cdx_toolkit.commoncrawl.apply_cc_defaults(test_in, now=now)
        assert test_in == test_out


my_cc_endpoints = [
    'http://index.commoncrawl.org/CC-MAIN-2013-20-index',
    'http://index.commoncrawl.org/CC-MAIN-2017-51-index',
    'http://index.commoncrawl.org/CC-MAIN-2018-05-index',
    'http://index.commoncrawl.org/CC-MAIN-2018-09-index',
    'http://index.commoncrawl.org/CC-MAIN-2018-13-index',
]


def test_customize_index_list():
    tests = [
        # gets the whole list because 201704 is before the first 2017 index
        [{'to': '201804'}, list(reversed(my_cc_endpoints))],
        [{'from_ts': '201801', 'to': '201804'}, my_cc_endpoints[4:0:-1]],
        [{'from_ts': '20180214', 'to': '201804'}, my_cc_endpoints[4:1:-1]],
        [{'from_ts': '20180429', 'to': '20180430'}, my_cc_endpoints[4:5]],
        # empty time range
        [{'from_ts': '20180430', 'to': '20180429'}, my_cc_endpoints[4:5]],
        # very empty time range
        [{'from_ts': '20180430', 'to': '20100429'}, []],
    ]

    with mock.patch('cdx_toolkit.get_cc_endpoints', return_value=my_cc_endpoints):
        cdx = cdx_toolkit.CDXFetcher(source='cc')
        cdxa = cdx_toolkit.CDXFetcher(source='cc', cc_sort='ascending')
        cdxb = cdx_toolkit.CDXFetcher(source='cc', cc_sort='invalid', loglevel='DEBUG')

        for params, custom_list in tests:
            cdx_toolkit.commoncrawl.apply_cc_defaults(params)
            assert cdx.customize_index_list(params) == custom_list
            assert cdxa.customize_index_list(params) == list(reversed(custom_list))
            with pytest.raises(ValueError):
                cdxb.customize_index_list(params)


def test_customize_index_list_closest():
    # when I implement the funky sort order, this will become different
    my_cc_endpoints_rev = list(reversed(my_cc_endpoints))
    tests = [
        [{'closest': '201801', 'from_ts': '20171230', 'to': None}, my_cc_endpoints_rev[0:4]],
        [{'closest': '201803', 'from_ts': '20180214', 'to': None}, my_cc_endpoints_rev[0:3]],
        [{'closest': '201801', 'from_ts': '20171230', 'to': '201802'}, my_cc_endpoints_rev[2:4]],
    ]

    with mock.patch('cdx_toolkit.get_cc_endpoints', return_value=my_cc_endpoints):
        cdx = cdx_toolkit.CDXFetcher(source='cc')

        for params, custom_list in tests:
            cdx_toolkit.commoncrawl.apply_cc_defaults(params)
            print(params)
            assert cdx.customize_index_list(params) == custom_list


def test_filter_cc_endpoints():
    # gets covered by testing customize_list_index
    pass
