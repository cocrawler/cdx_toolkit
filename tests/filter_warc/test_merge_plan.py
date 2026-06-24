"""Unit tests for the S3 merge part planner (cdx_toolkit.filter_warc.merge._plan_parts).

Pure byte-range arithmetic, no S3. These lock the two invariants the concurrent
UploadPartCopy merge depends on: full ordered byte coverage of every shard, and the
"every part except the overall last is >= 5 MiB" rule.
"""
from cdx_toolkit.filter_warc.merge import _plan_parts, _MIN_PART

MiB = 1024 * 1024


def _assert_covers(plan, sources_with_sizes):
    """Plan must reproduce each shard's [0, size) exactly, contiguous and in order."""
    by_src = {}
    for p in plan:
        by_src.setdefault(p['src'], []).append((p['start'], p['end']))
    for src, sz in sources_with_sizes:
        ranges = by_src.get(src, [])
        if sz == 0:
            assert ranges == [], 'zero-length shard should contribute no parts'
            continue
        assert ranges[0][0] == 0
        assert ranges[-1][1] == sz
        for (s, e), (ns, _ne) in zip(ranges, ranges[1:]):
            assert e == ns, 'ranges must be contiguous'
            assert e > s


def _assert_min_part_rule(plan):
    """Every part except the very last must be >= 5 MiB."""
    for p in plan[:-1]:
        assert (p['end'] - p['start']) >= _MIN_PART, (p, 'non-final part below 5 MiB')


def test_single_small_shard_one_part():
    src = [('s3://b/a', 3 * MiB)]
    plan = _plan_parts(src, part_target=256 * MiB)
    assert plan == [{'src': 's3://b/a', 'start': 0, 'end': 3 * MiB}]
    _assert_covers(plan, src)


def test_shard_splits_on_part_target():
    src = [('s3://b/a', 660 * MiB)]
    plan = _plan_parts(src, part_target=256 * MiB)
    assert [(p['start'], p['end']) for p in plan] == [
        (0, 256 * MiB), (256 * MiB, 512 * MiB), (512 * MiB, 660 * MiB)
    ]
    _assert_covers(plan, src)
    _assert_min_part_rule(plan)


def test_small_tail_folds_into_previous():
    # 256 MiB + 1 KiB: the 1 KiB tail would violate the 5 MiB rule, so fold it back.
    sz = 256 * MiB + 1024
    plan = _plan_parts([('s3://b/a', sz)], part_target=256 * MiB)
    assert plan == [{'src': 's3://b/a', 'start': 0, 'end': sz}]


def test_exact_multiple_no_fold():
    plan = _plan_parts([('s3://b/a', 512 * MiB)], part_target=256 * MiB)
    assert [(p['start'], p['end']) for p in plan] == [(0, 256 * MiB), (256 * MiB, 512 * MiB)]


def test_many_shards_ordered_and_complete():
    src = [('s3://b/sh%02d' % i, 660 * MiB) for i in range(16)]
    plan = _plan_parts(src, part_target=256 * MiB)
    _assert_covers(plan, src)
    _assert_min_part_rule(plan)
    # shards stay grouped and in order
    assert [p['src'] for p in plan[:3]] == ['s3://b/sh00'] * 3
    assert [p['src'] for p in plan[-3:]] == ['s3://b/sh15'] * 3


def test_final_shard_may_be_small():
    # A big first shard then a tiny final shard: the tiny shard is the global-last
    # part and is allowed to be < 5 MiB.
    src = [('s3://b/big', 300 * MiB), ('s3://b/tiny', 1024)]
    plan = _plan_parts(src, part_target=256 * MiB)
    _assert_covers(plan, src)
    assert plan[-1] == {'src': 's3://b/tiny', 'start': 0, 'end': 1024}
    _assert_min_part_rule(plan)


def test_large_shard_stays_under_5gib_ceiling():
    # 6 GiB shard (as a 100 GiB/16 job would produce) must split so no single part
    # exceeds the 5 GiB UploadPartCopy limit.
    plan = _plan_parts([('s3://b/a', 6 * 1024 * MiB)], part_target=256 * MiB)
    assert all((p['end'] - p['start']) <= 5 * 1024 * MiB for p in plan)
    _assert_covers(plan, [('s3://b/a', 6 * 1024 * MiB)])
    _assert_min_part_rule(plan)
