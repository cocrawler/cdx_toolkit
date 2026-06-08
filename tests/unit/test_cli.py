from argparse import Namespace
from unittest.mock import Mock, patch

import cdx_toolkit.cli
import cdx_toolkit.warc


def _make_fake_record():
    resp = Mock()
    resp.status_code = 200
    resp.reason = 'OK'
    resp.headers = {'Content-Type': 'text/html'}
    resp.content = b'<html>test</html>'
    return cdx_toolkit.warc.fake_wb_warc(
        url='http://example.com',
        wb_url='https://web.archive.org/web/20240101120000id_/http://example.com',
        resp=resp,
        capture={'url': 'http://example.com', 'timestamp': '20240101120000', 'status': '200'},
    )


def _build_warc_cmd(tmp_path, **overrides):
    cmd = Namespace(
        prefix=str(tmp_path / 'cli'),
        subprefix=None,
        url='http://example.com',
        url_fgrep=None,
        url_fgrepv=None,
        creator=None,
        operator=None,
    )
    for key, value in overrides.items():
        setattr(cmd, key, value)
    return cmd


def _make_capture(is_revisit=False, url='http://example.com'):
    capture = Mock()
    capture.__getitem__ = lambda self, key: {'url': url, 'timestamp': '20240101120000'}[key]
    capture.is_revisit.return_value = is_revisit
    capture.fetch_warc_record.return_value = _make_fake_record()
    return capture


def test_warcer_logs_revisit(tmp_path, caplog):
    """warcer should log a warning when iterating over a revisit capture."""
    cmd = _build_warc_cmd(tmp_path)
    fake_cdx = Mock()
    fake_cdx.iter.return_value = iter([_make_capture(is_revisit=True)])

    with patch.object(cdx_toolkit.cli, 'setup_cdx_fetcher_and_kwargs', return_value=(fake_cdx, {})):
        with caplog.at_level('WARNING', logger='cdx_toolkit.cli'):
            cdx_toolkit.cli.warcer(cmd, cmdline='test')

    assert 'revisit record being resolved' in caplog.text
    assert list(tmp_path.glob('cli-*.warc.gz'))


def test_warcer_skips_fgrep_and_fgrepv(tmp_path, caplog):
    """warcer should skip URLs not matching --url-fgrep or matching --url-fgrepv."""
    cmd = _build_warc_cmd(tmp_path, url_fgrep='wanted', url_fgrepv='forbidden')
    captures = [
        _make_capture(url='http://example.com/nope'),         # fails fgrep
        _make_capture(url='http://example.com/wanted/forbidden'),  # fails fgrepv
        _make_capture(url='http://example.com/wanted/keep'),  # kept
    ]
    fake_cdx = Mock()
    fake_cdx.iter.return_value = iter(captures)

    with patch.object(cdx_toolkit.cli, 'setup_cdx_fetcher_and_kwargs', return_value=(fake_cdx, {})):
        with caplog.at_level('DEBUG', logger='cdx_toolkit.cli'):
            cdx_toolkit.cli.warcer(cmd, cmdline='test')

    assert 'not warcing due to fgrep' in caplog.text
    assert 'not warcing due to fgrepv' in caplog.text
    assert captures[0].fetch_warc_record.call_count == 0
    assert captures[1].fetch_warc_record.call_count == 0
    assert captures[2].fetch_warc_record.call_count == 1


def test_warcer_passes_size_to_writer(tmp_path):
    """warcer should pop 'size' out of kwargs and forward it to the writer."""
    cmd = _build_warc_cmd(tmp_path)
    fake_cdx = Mock()
    fake_cdx.iter.return_value = iter([])
    captured = {}

    real_get_writer = cdx_toolkit.warc.get_writer

    def spy_get_writer(prefix, subprefix, info, **kwargs):
        captured.update(kwargs)
        return real_get_writer(prefix, subprefix, info, **kwargs)

    with patch.object(cdx_toolkit.cli, 'setup_cdx_fetcher_and_kwargs', return_value=(fake_cdx, {'size': 42})):
        with patch.object(cdx_toolkit.warc, 'get_writer', side_effect=spy_get_writer):
            cdx_toolkit.cli.warcer(cmd, cmdline='test')

    assert captured == {'size': 42}
