"""Lazy loading for ESA anomaly detection datasets.

Layout on disk: ``data/{mission}/{variant}/{split}/YYYY_MM.parquet``.

Example
-------
    from adlab.data import scan, downsample

    lf = scan('mission1', variant='z-normalized', split='test',
              t_start='2010-01-01', t_end='2010-02-01',
              columns=['channel_12'])
    df = lf.collect()

    overview = downsample(scan('mission1'), bucket='1d').collect()
"""

from datetime import datetime
from typing import Iterable, Optional, Union

import polars as pl

from adlab import ROOT

DATA_DIR = ROOT / "data"

Split = str  # 'train' | 'test' | 'both'
Timestamp = Union[str, datetime]


def scan(
    mission: str,
    variant: str = "raw",
    split: Split = "both",
    t_start: Optional[Timestamp] = None,
    t_end: Optional[Timestamp] = None,
    columns: Optional[Iterable[str]] = None,
) -> pl.LazyFrame:
    """Return a polars LazyFrame over the requested slice.

    Nothing is read from disk until ``.collect()`` (or ``.fetch()``) is called.
    Parquet column statistics let polars prune files that fall outside
    [t_start, t_end), so large range filters stay cheap.
    """
    splits = ("train", "test") if split == "both" else (split,)
    patterns = [str(DATA_DIR / mission / variant / s / "*.parquet") for s in splits]

    lf = pl.scan_parquet(patterns)

    if t_start is not None:
        lf = lf.filter(pl.col("timestamp") >= _to_dt(t_start))
    if t_end is not None:
        lf = lf.filter(pl.col("timestamp") < _to_dt(t_end))

    if columns is not None:
        cols = ["timestamp"] + [c for c in columns if c != "timestamp"]
        lf = lf.select(cols)

    return lf.sort("timestamp")


def downsample(
    lf: pl.LazyFrame,
    bucket: str = "1h",
    agg: str = "mean",
) -> pl.LazyFrame:
    """Bucket a LazyFrame by time for overview plots.

    ``bucket`` uses polars duration syntax ('30s', '5m', '1h', '1d', ...).
    ``agg`` is 'mean' | 'min' | 'max' | 'first' | 'last'.

    Non-numeric columns other than timestamp are dropped (aggregation is
    undefined for them).
    """
    schema = lf.collect_schema()
    numeric = [name for name, dtype in schema.items()
               if name != "timestamp" and dtype.is_numeric()]

    aggregator = getattr(pl.col(numeric), agg)
    return (
        lf.group_by_dynamic("timestamp", every=bucket)
          .agg(aggregator())
    )


def _to_dt(t: Timestamp) -> datetime:
    if isinstance(t, datetime):
        return t
    return datetime.fromisoformat(t)
