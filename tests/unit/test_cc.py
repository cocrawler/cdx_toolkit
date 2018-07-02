import cdx_toolkit.commoncrawl as cc


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
        cc.apply_cc_defaults(test_in, now=now)
        assert test_in == test_out
