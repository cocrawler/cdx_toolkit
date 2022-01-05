from cdx_toolkit.cli import main

import json
import sys

import pytest
import requests


def test_basics(capsys):
    args = '--cc --limit 10 iter commoncrawl.org/*'.split()
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


def test_multi_cc1(capsys, caplog):
    tests = [
        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'commoncrawl.org'}],
        [{'service': '--cc', 'mods': '--limit 11', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 11, 'linefgrep': 'commoncrawl.org'}],
#        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/thisurlneverdidexist'},
#         {'count': 0}],  # should limit to 1 index because it runs slowly!
        [{'service': '--cc', 'mods': '--cc-mirror https://index.commoncrawl.org/ --limit 11', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 11, 'linefgrep': 'commoncrawl.org'}],
        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/* --all-fields'},
         {'count': 10, 'linefgrep': 'digest '}],
        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/* --fields=digest,length,offset --csv'},
         {'count': 11, 'csv': True}],
        [{'service': '--cc', 'mods': '--limit 10 --filter=status:200', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'status 200'}],
        [{'service': '--cc', 'mods': '--limit 10 --filter=!status:200 --filter=!status:404', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrepv': 'status 200'}],
        [{'service': '--cc', 'mods': '--limit 10 --to=2017', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'timestamp 2017'}],
        [{'service': '--cc', 'mods': '--limit 10 --from=2017 --to=2017', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'timestamp 2017'}],
    ]

    for t in tests:
        multi_helper(t, capsys, caplog)


def test_multi_cc2(capsys, caplog):
    tests = [
        [{'service': '--cc', 'mods': '--limit 3 --get --closest=20170615', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 3, 'linefgrep': 'timestamp 20170'}],  # data-dependent, and kinda broken
        [{'service': '--cc', 'mods': '--limit 3 --get --filter status:200 --closest=20170615', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 3, 'linefgrep': 'timestamp 20170'}],  # data-dependent, and kinda broken
        [{'service': '--cc', 'mods': '--get --closest=20170615', 'cmd': 'iter', 'rest': 'commoncrawl.org/never-existed'},
         {'count': 0}],

        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/* --csv'},
         {'count': 11, 'csv': True}],
        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/* --jsonl'},
         {'count': 10, 'jsonl': True}],
        [{'service': '--cc', 'mods': '-v -v --limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'debug': 5}],
    ]

    for t in tests:
        multi_helper(t, capsys, caplog)


def test_multi_ia(capsys, caplog):
    tests = [
        [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'commoncrawl.org'}],
        [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/thisurlneverdidexist'},
         {'count': 0}],
        [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/* --all-fields'},
         {'count': 10, 'linefgrep': 'mime ', 'linefgrepv': 'original '}],  # both of these are renamed fields
        [{'service': '--ia', 'mods': '--get --limit 4 --closest=20170615', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 4, 'linefgrep': 'timestamp '}],  # returns 2008 ?! bug probably on my end
        [{'service': '--ia', 'mods': '-v -v --limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'debug': 5}],
    ]

    for t in tests:
        multi_helper(t, capsys, caplog)


def test_multi_rest(capsys, caplog):
    tests = [
        [{'service': '--source https://web.archive.org/cdx/search/cdx', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'count': 10, 'linefgrep': 'commoncrawl.org'}],
        [{'service': '-v -v --source https://web.arc4567hive.org/cdx/search/cdx', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}],
        [{'service': '-v -v --source https://example.com/404', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}],

        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'size', 'rest': 'commoncrawl.org/*'},
         {'count': 1, 'is_int': True}],
        [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'size', 'rest': 'commoncrawl.org/*'},
         {'count': 1, 'is_int': True}],
        [{'service': '--cc', 'mods': '--limit 10', 'cmd': 'size', 'rest': '--details commoncrawl.org/*'},
         {'count': 2}],
        [{'service': '--ia', 'mods': '--limit 10', 'cmd': 'size', 'rest': '--details commoncrawl.org/*'},
         {'count': 2}],
        [{'service': '--ia', 'mods': '--from 20180101 --to 20180110 --limit 10', 'cmd': 'size', 'rest': '--details commoncrawl.org'},
         {'count': 2}],

        [{'service': '', 'mods': '--limit 10', 'cmd': 'iter', 'rest': 'commoncrawl.org/*'},
         {'exception': ValueError}],
    ]

    for t in tests:
        multi_helper(t, capsys, caplog)


def test_warc(tmpdir, caplog):
    # crash testing only, so far

    base = ' --limit 10 warc commoncrawl.org/*'

    prefixes = ('-v -v --cc', '--ia',
                '--cc --cc-mirror https://index.commoncrawl.org/',
                '--source https://web.archive.org/cdx/search/cdx --wb https://web.archive.org/web')
    suffixes = ('--prefix FOO --subprefix BAR --size 1 --creator creator --operator bob --url-fgrep common --url-fgrepv bar',
                '--prefix EMPTY --size 1 --url-fgrep bar',
                '--prefix EMPTY --size 1 --url-fgrepv common')

    with tmpdir.as_cwd():
        for p in prefixes:
            cmdline = p + base
            print(cmdline, file=sys.stderr)
            args = cmdline.split()
            main(args=args)

        for s in suffixes:
            cmdline = prefixes[0] + base + ' ' + s
            print(cmdline, file=sys.stderr)
            args = cmdline.split()
            main(args=args)

        assert True


def one_ia_corner(tmpdir, cmdline):
    with tmpdir.as_cwd():
        main(args=cmdline.split())


def test_warc_ia_corners(tmpdir, caplog):
    '''
    To test these more properly, need to add a --exact-warcname and then postprocess.
    For now, these tests show up in the coverage report
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
