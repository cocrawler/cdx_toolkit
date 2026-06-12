import asyncio
import csv

from cdx_toolkit.filter_warc.data_classes import RangeJob
from cdx_toolkit.filter_warc.sources.base import RangeJobSource
from cdx_toolkit.filter_warc.warc_filter import WARCFilter, _STOP


class FakeSource(RangeJobSource):
    def __init__(self, jobs, raise_after=None):
        self.jobs = jobs
        self.raise_after = raise_after

    def iter_range_jobs(self):
        for i, job in enumerate(self.jobs):
            if self.raise_after is not None and i == self.raise_after:
                raise RuntimeError('boom')
            yield job


def _jobs(n):
    return [
        RangeJob(url=f'https://data.commoncrawl.org/{i}.warc.gz', offset=i, length=1, filename=f'{i}.warc.gz')
        for i in range(n)
    ]


def test_no_fetch_materializes_csv(tmp_path):
    out = str(tmp_path / 'ranges.csv')
    wf = WARCFilter(
        source=FakeSource(_jobs(3)),
        prefix_path=str(tmp_path / 'out'),
        writer_info={'isPartOf': 'test'},
        range_jobs_output=out,
        no_fetch=True,
    )
    n = wf.filter()
    assert n == 3
    with open(out, newline='') as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 3
    assert set(rows[0].keys()) == {'filename', 'offset', 'length'}


def test_producer_emits_stops_even_when_source_raises(tmp_path):
    """Regression: a source that raises mid-iteration must still release the readers."""
    wf = WARCFilter(
        source=FakeSource(_jobs(2), raise_after=1),
        prefix_path=str(tmp_path),
        writer_info={},
        n_parallel=3,
    )

    async def run():
        queue: asyncio.Queue = asyncio.Queue()
        try:
            await wf._produce_range_jobs(queue, None)
        except RuntimeError:
            pass
        items = []
        while not queue.empty():
            items.append(await queue.get())
        return items

    items = asyncio.run(run())
    stops = sum(1 for it in items if it is _STOP)
    assert wf.num_readers == 3
    assert stops == 3
