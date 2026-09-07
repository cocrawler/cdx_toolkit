from unittest.mock import patch

import pytest

from cdx_toolkit.filter_warc.command import confirm_athena_cost


def test_small_crawl_set_no_prompt():
    # <= LARGE_CRAWL_SET_THRESHOLD -> considered cheap, no prompt
    with patch('builtins.input') as inp:
        confirm_athena_cost(n_crawls=5, confirmed=False)
        inp.assert_not_called()


def test_confirmed_flag_bypasses_prompt():
    with patch('builtins.input') as inp:
        confirm_athena_cost(n_crawls=None, confirmed=True)
        inp.assert_not_called()


def test_large_crawl_set_non_tty_aborts():
    with patch('cdx_toolkit.filter_warc.command.sys.stdin') as stdin:
        stdin.isatty.return_value = False
        with pytest.raises(SystemExit):
            confirm_athena_cost(n_crawls=11, confirmed=False)


def test_unknown_crawls_non_tty_aborts():
    with patch('cdx_toolkit.filter_warc.command.sys.stdin') as stdin:
        stdin.isatty.return_value = False
        with pytest.raises(SystemExit):
            confirm_athena_cost(n_crawls=None, confirmed=False)


def test_tty_yes_proceeds():
    with patch('cdx_toolkit.filter_warc.command.sys.stdin') as stdin, patch('builtins.input', return_value='y'):
        stdin.isatty.return_value = True
        confirm_athena_cost(n_crawls=None, confirmed=False)  # should not raise


def test_tty_empty_answer_aborts():
    with patch('cdx_toolkit.filter_warc.command.sys.stdin') as stdin, patch('builtins.input', return_value=''):
        stdin.isatty.return_value = True
        with pytest.raises(SystemExit):
            confirm_athena_cost(n_crawls=None, confirmed=False)


def test_tty_no_answer_aborts():
    with patch('cdx_toolkit.filter_warc.command.sys.stdin') as stdin, patch('builtins.input', return_value='n'):
        stdin.isatty.return_value = True
        with pytest.raises(SystemExit):
            confirm_athena_cost(n_crawls=11, confirmed=False)
