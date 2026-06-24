"""Hugging Face Storage Bucket support for repackage reads/writes.

HF Storage Buckets (``hf://buckets/<namespace>/<name>/<key>``) expose data only
through the Hugging Face filesystem / HTTP resolve API -- there is no S3 API yet.
So cdxt cannot reach a bucket via the aioboto3 read path. Instead a WARC byte
range can be fetched in one of two ways, selected with ``--hf-reader``:

- ``fsspec`` (default): ``HfFileSystem.cat_file(path, start, end)`` run in a worker
  thread (HfFileSystem is synchronous). Handles auth, Xet dedup and the CDN
  transparently; the simplest path.
- ``cdn``: an async ``aiohttp`` ranged ``GET`` against the CDN-fronted resolve URL
  (``https://huggingface.co/buckets/<ns>/<name>/resolve/<key>``). Fits the async
  reader model and exercises the bucket CDN directly.

Both return the exact ``length`` bytes for ``offset`` (verified byte-identical to
the same record read from ``s3://commoncrawl``).
"""
import asyncio
import contextlib
import logging
import re

logger = logging.getLogger(__name__)

HF_SCHEME = 'hf://'
HF_ENDPOINT = 'https://huggingface.co'

# HF read implementations selectable via --hf-reader.
HF_READERS = ('fsspec', 'cdn')


def is_hf_url(url) -> bool:
    """True for an ``hf://`` URL (e.g. an hf:// WARC download prefix or job URL)."""
    return bool(url) and url.startswith(HF_SCHEME)


def hf_fs_path(url: str) -> str:
    """``hf://buckets/ns/name/key`` -> ``buckets/ns/name/key`` (HfFileSystem path)."""
    if not is_hf_url(url):
        raise ValueError(f'Not an hf:// URL: {url}')
    return url[len(HF_SCHEME):]


def hf_resolve_url(url: str) -> str:
    """Map an ``hf://`` bucket URL to its CDN-fronted HTTP(S) resolve URL.

    ``hf://buckets/<ns>/<name>/<key>``
        -> ``https://huggingface.co/buckets/<ns>/<name>/resolve/<key>``
    """
    path = hf_fs_path(url)
    parts = path.split('/', 3)
    if len(parts) < 4 or parts[0] != 'buckets':
        raise ValueError(
            f'Unsupported hf:// URL (expected hf://buckets/<ns>/<name>/<key>): {url}'
        )
    repo = '/'.join(parts[:3])  # buckets/<ns>/<name>
    key = parts[3]
    return f'{HF_ENDPOINT}/{repo}/resolve/{key}'


def retry_after_seconds(headers, default: float) -> float:
    """Seconds to wait after a 429, from the response headers.

    Prefers ``Retry-After`` (delta-seconds); else the ``t=`` field of the
    standardized ``RateLimit`` header (seconds until the window resets); else
    ``default``.
    """
    ra = headers.get('Retry-After')
    if ra:
        try:
            return max(0.0, float(ra))
        except (TypeError, ValueError):
            pass
    m = re.search(r'(?:^|[;,\s])t\s*=\s*(\d+)', headers.get('RateLimit', '') or '')
    if m:
        return float(m.group(1))
    return default


class HfFsspecReader:
    """Ranged reads via the (synchronous) ``HfFileSystem``, bridged into asyncio."""

    mode = 'fsspec'

    def __init__(self):
        from huggingface_hub import HfFileSystem

        self.fs = HfFileSystem()

    async def ranged_get(self, url: str, offset: int, length: int, **_kw) -> bytes:
        path = hf_fs_path(url)
        # fsspec cat_file end is exclusive.
        return await asyncio.to_thread(self.fs.cat_file, path, offset, offset + length)

    async def aclose(self):
        pass


class HfHttpReader:
    """Async ranged ``GET`` against the CDN-fronted HF resolve URL (aiohttp).

    Each record fetch is one ``/resolve/`` request, which counts against the HF
    Resolvers rate limit. On ``429`` this reader honours the server-provided wait
    (``Retry-After`` / ``RateLimit`` header) instead of failing, and tallies the
    throttle events/wait so a run reports whether it was rate-limited (the limit,
    not CPU/CDN, is the binding constraint at high record rates).
    """

    mode = 'cdn'

    # Bound the wait-out so a job can ride out a 5-minute window but not hang
    # forever if the limit never clears.
    MAX_THROTTLE_WAIT = 360.0  # cap a single 429 backoff (seconds)
    MAX_THROTTLE_RETRIES = 60  # consecutive 429s before giving up on a record

    def __init__(self, session, token=None):
        self.session = session
        self.token = token
        self.throttle_count = 0       # number of 429s seen
        self.throttle_wait_s = 0.0    # cumulative time spent backing off 429s
        self._policy_logged = False

    async def ranged_get(
        self,
        url: str,
        offset: int,
        length: int,
        *,
        max_attempts: int = 5,
        base_backoff_seconds: float = 0.5,
    ) -> bytes:
        resolve = hf_resolve_url(url)
        end = offset + length - 1  # inclusive HTTP Range
        headers = {'Range': f'bytes={offset}-{end}'}
        if self.token:
            headers['Authorization'] = f'Bearer {self.token}'

        attempt = 0
        throttles = 0
        last_exc = None
        while True:
            try:
                async with self.session.get(resolve, headers=headers) as resp:
                    if resp.status == 429:
                        throttles += 1
                        self.throttle_count += 1
                        default = min(30.0, base_backoff_seconds * 2 ** min(throttles, 6))
                        wait = retry_after_seconds(resp.headers, default)
                        if not self._policy_logged:
                            self._policy_logged = True
                            logger.warning(
                                'HF resolver rate limit hit (429); RateLimit=%r Policy=%r. '
                                'Throughput is now capped by the Resolvers quota.',
                                resp.headers.get('RateLimit'),
                                resp.headers.get('RateLimit-Policy'),
                            )
                        await resp.release()
                        if throttles > self.MAX_THROTTLE_RETRIES or wait > self.MAX_THROTTLE_WAIT:
                            raise OSError(
                                f'HF 429: gave up after {throttles} throttle waits '
                                f'(last wait {wait:.0f}s) for {resolve}'
                            )
                        self.throttle_wait_s += wait
                        await asyncio.sleep(wait)
                        continue  # does not consume the hard-error attempt budget
                    if resp.status not in (200, 206):
                        body = await resp.read()
                        raise OSError(f'HF GET {resolve} -> {resp.status}: {body[:200]!r}')
                    data = await resp.read()
                if len(data) != length:
                    raise OSError(f'HF GET {resolve} returned {len(data)} bytes, expected {length}')
                return data
            except Exception as exc:  # noqa: BLE001 - retry any transient read error
                last_exc = exc
                attempt += 1
                if attempt >= max_attempts:
                    break
                await asyncio.sleep(min(8.0, base_backoff_seconds * 2 ** (attempt - 1)))
        raise last_exc

    async def aclose(self):
        if self.throttle_count:
            logger.warning(
                'HF resolver throttling: %d x 429, %.1fs total spent backing off '
                '(read throughput was rate-limit bound)',
                self.throttle_count,
                self.throttle_wait_s,
            )


@contextlib.asynccontextmanager
async def make_hf_reader(mode: str):
    """Async context manager yielding the configured HF reader.

    ``cdn`` owns an ``aiohttp`` session for its lifetime; ``fsspec`` holds a shared
    ``HfFileSystem``. Imports are lazy so the optional ``hf`` extra is only required
    when actually reading from a bucket.
    """
    if mode == 'cdn':
        import aiohttp
        from huggingface_hub import get_token

        timeout = aiohttp.ClientTimeout(total=300, sock_connect=15)
        connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            reader = HfHttpReader(session, token=get_token())
            try:
                yield reader
            finally:
                await reader.aclose()
    else:
        reader = HfFsspecReader()
        try:
            yield reader
        finally:
            await reader.aclose()
