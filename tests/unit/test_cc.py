import unittest.mock as mock
import pytest

import cdx_toolkit.commoncrawl
from cdx_toolkit.timeutils import timestamp_to_time

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
    'https://index.commoncrawl.org/CC-MAIN-2013-20-index',
    'https://index.commoncrawl.org/CC-MAIN-2017-51-index',
    'https://index.commoncrawl.org/CC-MAIN-2018-05-index',
    'https://index.commoncrawl.org/CC-MAIN-2018-09-index',
    'https://index.commoncrawl.org/CC-MAIN-2018-13-index',
]
'''
    'https://index.commoncrawl.org/CC-MAIN-2012-index',
    'https://index.commoncrawl.org/CC-MAIN-2009-2010-index',
    'https://index.commoncrawl.org/CC-MAIN-2008-2009-index',
]
'''


def test_make_cc_maps():
    cc_map, cc_times = cdx_toolkit.commoncrawl.make_cc_maps(my_cc_endpoints)
    t = cc_times[0]
    assert cc_map[t] == 'https://index.commoncrawl.org/CC-MAIN-2013-20-index'
    t = cc_times[-1]
    assert cc_map[t] == 'https://index.commoncrawl.org/CC-MAIN-2018-13-index'


def test_check_cc_from_to():
    params_that_raise = [
        {'closest': '2010', 'to': '2010'},  # needs from
        {'closest': '2010', 'from_ts': '2010'},  # needs to
        {'to': '2010'},  # needs from_ts
        {},  # needs 'from_ts'
    ]
    for params in params_that_raise:
        with pytest.raises(ValueError):
            cdx_toolkit.commoncrawl.check_cc_from_to(params)


def test_bisect_cc():
    cc_map, cc_times = cdx_toolkit.commoncrawl.make_cc_maps(my_cc_endpoints)

    tests = [
        #[(from, to), (first, last)],
        [('201801', '201804'), ('2017-51', '2018-13', 4)],  # XXX one too many at start
        [('20180214', '201804'), ('2018-05', '2018-13', 3)],  # XXX one too many at start
        [('20180429', '20180430'), ('2018-13', '2018-13', 1)],  # XXX one too early for start and end XXX should be visible in cli... cli shows 2018-{17,13,9}
        #[('', ''), ('', '')],
        #[('', ''), ('', '')],
    ]

    i_last = sorted(my_cc_endpoints)[-1]

    for t in tests:
        from_ts_t = timestamp_to_time(t[0][0])
        to_t = timestamp_to_time(t[0][1])
        i_from = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][0])
        i_to = 'https://index.commoncrawl.org/CC-MAIN-{}-index'.format(t[1][1])
        i_count = t[1][2]

        index_list = cdx_toolkit.commoncrawl.bisect_cc(cc_map, cc_times, from_ts_t, to_t)
        assert index_list[0] == i_from, 'test: '+repr(t)
        assert index_list[-1] == i_to, 'test: '+repr(t)
        assert len(index_list) == i_count

        index_list = cdx_toolkit.commoncrawl.bisect_cc(cc_map, cc_times, from_ts_t, None)
        assert index_list[0] == i_from, 'test: '+repr(t)
        assert index_list[-1] == i_last, 'test: '+repr(t)
        assert len(index_list) >= i_count


def test_customize_index_list():
    tests = [
        # XXX change to first, last count?

        # gets the whole list because 201704 is before the first 2017 index
        # XXX why is 2013-20 being included ?!
        [{'to': '201804'}, list(reversed(my_cc_endpoints))],

        [{'from_ts': '201801', 'to': '201804'}, my_cc_endpoints[4:0:-1]],  # gets 2017-51 but not 2013-20
        [{'from_ts': '20180214', 'to': '201804'}, my_cc_endpoints[4:1:-1]],  # does not get 2017-51, does 2018-05 XXX
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
