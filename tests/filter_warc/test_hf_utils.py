"""Offline unit tests for hf:// bucket URL helpers (no network)."""
import pytest

from cdx_toolkit.filter_warc.hf_utils import (
    is_hf_url,
    hf_fs_path,
    hf_resolve_url,
    retry_after_seconds,
    HF_READERS,
)


def test_is_hf_url():
    assert is_hf_url('hf://buckets/commoncrawl/commoncrawl/crawl-data/x.warc.gz')
    assert not is_hf_url('s3://commoncrawl/crawl-data/x.warc.gz')
    assert not is_hf_url('https://data.commoncrawl.org/x.warc.gz')
    assert not is_hf_url('')
    assert not is_hf_url(None)


def test_hf_fs_path_strips_scheme():
    assert hf_fs_path('hf://buckets/ns/name/a/b.warc.gz') == 'buckets/ns/name/a/b.warc.gz'


def test_hf_fs_path_rejects_non_hf():
    with pytest.raises(ValueError):
        hf_fs_path('s3://bucket/key')


def test_hf_resolve_url_maps_bucket_to_resolve():
    url = 'hf://buckets/commoncrawl/commoncrawl/crawl-data/CC-MAIN-2026-21/seg/x.warc.gz'
    assert hf_resolve_url(url) == (
        'https://huggingface.co/buckets/commoncrawl/commoncrawl/resolve/'
        'crawl-data/CC-MAIN-2026-21/seg/x.warc.gz'
    )


def test_hf_resolve_url_rejects_non_bucket():
    # only hf://buckets/<ns>/<name>/<key> is supported for resolve mapping
    with pytest.raises(ValueError):
        hf_resolve_url('hf://datasets/foo/bar/x.warc.gz')
    with pytest.raises(ValueError):
        hf_resolve_url('hf://buckets/onlytwo/segments')


def test_hf_readers_constant():
    assert HF_READERS == ('fsspec', 'cdn')


def test_retry_after_prefers_retry_after_header():
    assert retry_after_seconds({'Retry-After': '12'}, default=99) == 12.0


def test_retry_after_parses_ratelimit_t_field():
    h = {'RateLimit': '"resolvers";r=0;t=287'}
    assert retry_after_seconds(h, default=99) == 287.0


def test_retry_after_falls_back_to_default():
    assert retry_after_seconds({}, default=7.5) == 7.5
    # malformed Retry-After falls through to default (no RateLimit present)
    assert retry_after_seconds({'Retry-After': 'soon'}, default=3.0) == 3.0
