import json
import sys
import os
import platform
import logging

import pytest

from cdx_toolkit.cli import main

from .conftest import conditional_mock_responses


LOGGER = logging.getLogger(__name__)


def slow_ci():
    '''
    For Github Actions, the windows and macos runners are very slow.
    Detect those runners, so that we can cut testing short.
    '''
    if os.environ.get('FAKE_GITHUB_ACTION'):
        LOGGER.error('limiting pytest because FAKE_GITHUB_ACTION')
        return True
    if os.environ.get('GITHUB_ACTION'):
        if platform.system() in {'Darwin', 'Windows'}:
            LOGGER.error('limiting pytest because GITHUB_ACTION')
            return True
    v = sys.version_info
    if os.environ.get('GITHUB_ACTION') and v.major == 3 and v.minor != 12:
        LOGGER.error('limiting pytest because GITHUB_ACTION and py != 3.12')
        return True
    LOGGER.error('full pytest')


@conditional_mock_responses
def test_basics(capsys):
    args = '--cc --limit 10 iter commoncrawl.org/*'.split()
    main(args=args)
    out, err = capsys.readouterr()

    split = out.splitlines()
    assert len(split) == 10
    for line in out.splitlines():
        # this might be commoncrawl.org./ or commoncrawl.org/
        assert 'commoncrawl.org' in line

@conditional_mock_responses
def test_basics_2(capsys):
    args = '--crawl 2 --limit 10 iter commoncrawl.org/*'.split()
    main(args=args)
    out, err = capsys.readouterr()

    split = out.splitlines()
    assert len(split) == 10
    for line in out.splitlines():
        # this might be commoncrawl.org./ or commoncrawl.org/
        assert 'commoncrawl.org' in line


def multi_helper(t, capsys, caplog):
    inputs = t[0]
    outputs = t[1]
    cmdline = '{} {} {} {}'.format(inputs['service'], inputs['mods'], inputs['cmd'], inputs['rest'])
    args = cmdline.split()

    if 'exception' in outputs:
        with pytest.raises(outputs['exception']):
            main(args=args)
    else:
        main(args=args)

    out, err = capsys.readouterr()

    assert err == '', cmdline
    lines = out.splitlines()
    if 'count' in outputs:
        assert len(lines) == outputs['count'], cmdline
    for line in lines:
        if 'linefgrep' in outputs:
            assert outputs['linefgrep'] in line, cmdline
        if 'linefgrepv' in outputs:
            assert outputs['linefgrepv'] not in line, cmdline
        if 'csv' in outputs:
            assert line.count(',') >= 2, cmdline
        if 'jsonl' in outputs:
            assert line.startswith('{') and line.endswith('}'), cmdline
            assert json.loads(line), cmdline
        if 'is_int' in outputs:
            assert line.isdigit(), cmdline

    if 'debug' in outputs:
        assert len(caplog.records) > outputs['debug'], cmdline


@conditional_mock_responses
def test_multi_cc1(capsys, caplog):
    # this is the test case before slow_ci -> break
    testdata = [
                {
                    "service": "--cc",
                    "mods": "--limit 10",
                    "cmd": "iter",
                    "rest": "commoncrawl.org/*",
                },
                {"count": 10, "linefgrep": "commoncrawl.org"},
            ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_1(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 11",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 11, "linefgrep": "commoncrawl.org"},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_2(capsys, caplog):
    testdata = [
        {
            "service": "--crawl 1",
            "mods": "--limit 10",
            "cmd": "iter",
            "rest": "commoncrawl.org/thisurlneverdidexist",
        },
        {"count": 0},
    ]  # runs slowly if we don't limit crawl to 1
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_3(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--cc-mirror https://index.commoncrawl.org/ --limit 11",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 11, "linefgrep": "commoncrawl.org"},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_4(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10",
            "cmd": "iter",
            "rest": "commoncrawl.org/* --all-fields",
        },
        {"count": 10, "linefgrep": "digest "},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_5(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10",
            "cmd": "iter",
            "rest": "commoncrawl.org/* --fields=digest,length,offset --csv",
        },
        {"count": 11, "csv": True},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_6(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10 --filter=status:200",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 10, "linefgrep": "status 200"},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_7(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10 --filter=!status:200 --filter=!status:404",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 10, "linefgrepv": "status 200"},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_8(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10 --to=2017",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 10, "linefgrep": "timestamp 2017"},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc1_slow_9(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10 --from=2017 --to=2017",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 10, "linefgrep": "timestamp 2017"},
    ]
    multi_helper(testdata, capsys, caplog)

@conditional_mock_responses
def test_multi_cc2(capsys, caplog):
    # this is the test case before slow_ci -> break
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 3 --get --closest=20170615",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 3, "linefgrep": "timestamp 20170"},
    ]  # data-dependent, and kinda broken
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc2_slow_1(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 3 --get --filter status:200 --closest=20170615",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 3, "linefgrep": "timestamp 20170"},
    ]  # data-dependent, and kinda broken
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc2_slow_2(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--get --closest=20170615",
            "cmd": "iter",
            "rest": "commoncrawl.org/never-existed",
        },
        {"count": 0},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc2_slow_3(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10",
            "cmd": "iter",
            "rest": "commoncrawl.org/* --csv",
        },
        {"count": 11, "csv": True},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc2_slow_4(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "--limit 10",
            "cmd": "iter",
            "rest": "commoncrawl.org/* --jsonl",
        },
        {"count": 10, "jsonl": True},
    ]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_cc2_slow_5(capsys, caplog):
    testdata = [
        {
            "service": "--cc",
            "mods": "-v -v --limit 10",
            "cmd": "iter",
            "rest": "commoncrawl.org/*",
        },
        {"count": 10, "debug": 5},
    ]
    multi_helper(testdata, capsys, caplog)


@conditional_mock_responses
def test_multi_ia(capsys, caplog):
    testdata = [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'commoncrawl.org'}]
    # Disabled: minimize IA for ratelimit purposes
    # [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/thisurlneverdidexist'},
    #  {'count': 0}],
    # [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/* --all-fields'},
    #  {'count': 10, 'linefgrep': 'mime ', 'linefgrepv': 'original '}],  # both of these are renamed fields
    # [{'service': '--ia', 'mods': '--get --limit 4 --closest=20170615', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
    #  {'count': 4, 'linefgrep': 'timestamp '}],  # returns 2008 ?! bug probably on my end
    # [{'service': '--ia', 'mods': '-v -v --limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
    #  {'count': 10, 'debug': 5}]
    multi_helper(testdata, capsys, caplog)


def test_multi_misc_not_ia(capsys, caplog):
    # this is the test case before slow_ci -> break
    testdata = [{'service': '-v -v --source https://web.arc4567hive.org/cdx/search/cdx', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_misc_not_ia_slow_1(capsys, caplog):
    testdata = [{'service': '-v -v --source https://example.com/404', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_misc_not_ia_slow_2(capsys, caplog):
    testdata = [{'service': '--crawl 1,1', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_misc_not_ia_slow_3(capsys, caplog):
    testdata = [{'service': '--crawl 1,CC-MAIN-2024', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_misc_not_ia_slow_4(capsys, caplog):
    testdata = [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'size', 'rest': 'commoncrawl.org/*'},
         {'count': 1, 'is_int': True}]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_misc_not_ia_slow_5(capsys, caplog):
    testdata = [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'size', 'rest': '--details commoncrawl.org/*'},
         {'count': 2}]
    multi_helper(testdata, capsys, caplog)


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_multi_misc_not_ia_slow_6(capsys, caplog):
    testdata = [{'service': '', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}]
    multi_helper(testdata, capsys, caplog)


@conditional_mock_responses
def test_multi_misc_ia(capsys, caplog):
    testdata = [{'service': '--source https://web.archive.org/cdx/search/cdx', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'commoncrawl.org'}]
    # Disabled: minimize IA for ratelimit reasons
    # [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'size', 'rest': 'commoncrawl.org/*'},
    #  {'count': 1, 'is_int': True}],
    # [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'size', 'rest': '--details commoncrawl.org/*'},
    #  {'count': 2}],
    # [{'service': '--ia', 'mods': '--from 20180101 --to 20180110 --limit 10', 'cmd': 'size', 'rest': '--details commoncrawl.org'},
    #  {'count': 2}],
    multi_helper(testdata, capsys, caplog)


def warc_prefix_test_helper(tmpdir, prefix: str):
    # crash testing only, so far
    base = ' --limit 1 warc commoncrawl.org/*'

    with tmpdir.as_cwd():
        cmdline = prefix + base
        if 'cc' in cmdline:
            cmdline = cmdline.replace(' 1', ' 2')
        print(cmdline, file=sys.stderr)
        args = cmdline.split()
        main(args=args)


@conditional_mock_responses
def test_warc_prefix_1(tmpdir):
    warc_prefix_test_helper(tmpdir, '-v -v --cc')


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_warc_prefix_2(tmpdir):
    warc_prefix_test_helper(tmpdir, '--ia')


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_warc_prefix_3(tmpdir):
    warc_prefix_test_helper(tmpdir, '--cc --cc-mirror https://index.commoncrawl.org/')


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_warc_prefix_4(tmpdir):
    warc_prefix_test_helper(tmpdir, '--source https://web.archive.org/cdx/search/cdx --wb https://web.archive.org/web')


def warc_suffix_test_helper(tmpdir, suffix: str):
    # crash testing only, so far
    base = ' --limit 1 warc commoncrawl.org/*'
    prefix = '-v -v --cc'

    with tmpdir.as_cwd():
        cmdline = prefix + base + ' ' + suffix
        print(cmdline, file=sys.stderr)
        args = cmdline.split()
        main(args=args)


@conditional_mock_responses
def test_warc_suffix_1(tmpdir):
    warc_suffix_test_helper(tmpdir, '--prefix FOO --subprefix BAR --size 1 --creator creator --operator bob --url-fgrep common --url-fgrepv bar')


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_warc_suffix_2(tmpdir):
    warc_suffix_test_helper(tmpdir, '--prefix EMPTY --size 1 --url-fgrep bar')


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_warc_suffix_3(tmpdir):
    warc_suffix_test_helper(tmpdir, '--prefix EMPTY --size 1 --url-fgrepv common')


@pytest.mark.skipif(slow_ci(), reason="Slow CI")
@conditional_mock_responses
def test_warc_suffix_4(tmpdir):
    warc_suffix_test_helper(tmpdir, '--prefix FOO --subprefix BAR --size 1 --creator creator --operator bob --url-fgrep common --url-fgrepv bar')


def one_ia_corner(tmpdir, cmdline):
    with tmpdir.as_cwd():
        main(args=cmdline.split())


@pytest.mark.skip(reason='needs some ratelimit love')
def test_warc_ia_corners(tmpdir, caplog):
    '''
    To test these more properly, need to add a --exact-warcname and then postprocess.
    For now, these are only crash tests.
    '''

    # revisit vivification
    cmdline = '--ia --from 2017010118350 --to 2017010118350 warc pbm.com/robots.txt'
    one_ia_corner(tmpdir, cmdline)

    # same-surt same-timestamp redir+200
    cmdline = '--ia --from 20090220001146 --to 20090220001146 warc pbm.com'
    one_ia_corner(tmpdir, cmdline)

    # any redir -> 302, this is a 301
    cmdline = '--ia --from 2011020713024 --to 2011020713024 warc pbm.com'
    one_ia_corner(tmpdir, cmdline)

    # warcing a 404 is a corner case in myrequests
    cmdline = '--ia --from 20080512074145 --to 20080512074145 warc http://www.pbm.com/oly/archive/design94/0074.html'
    one_ia_corner(tmpdir, cmdline)
