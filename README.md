# DL Lab - Anomaly Detection (ESA-ADB)

This repository is designed for the Machine Learning for Anomaly Detection lab. It contains tools and datasets based on the European Space Agency Anomaly Detection Benchmark (ESA-ADB).

## Repository Structure

- `src/adlab/`: Installable Python package with shared code (models, features, data loader). Exposes `ROOT` (project root) so paths work from any script.
- `data/`: Processed datasets in Parquet format, partitioned as `{mission}/{variant}/{split}/YYYY_MM.parquet`. Gitignored.
- `notebooks/`: Jupyter notebooks for analysis and experimentation.
- `_scripts/`: Standalone scripts (dataset generation, one-off utilities).
- `esa-adb/`: Original ESA-ADB data (gitignored).
- `environment.yml` / `Makefile`: Conda env + editable install of `adlab`.

## Environment Setup

**Prerequisite:** Anaconda/Miniconda installed.

First-time setup (run from the repo root):

```bash
make create_env                                # creates the env AND pip-installs adlab in editable mode
conda activate dl-lab-anomaly-detection
```

To update after pulling changes:

```bash
make update_env
```

Both `make create_env` and `make update_env` install `adlab` in editable mode (`pip install -e .`) via the `environment.yml` pip section. You don't need to run any install step separately.

**IDE note:** point PyCharm/VSCode's interpreter to the `dl-lab-anomaly-detection` conda env — imports like `from adlab import ROOT` resolve through the interpreter, so there's no need to mark `src` as a sources root.

## Datasets

Detailed information about the datasets (Raw, Z-normalized) and their generation process can be found in [data/README.md](data/README.md).

### Unpack the provided archive

Place `data.zip` at the repository root and extract it in place:

```bash
unzip data.zip
```

The archive already contains a top-level `data/` directory, so this produces `data/{mission}/{variant}/{split}/YYYY_MM.parquet` directly. If the `data/` folder already exists, `unzip` will prompt before overwriting — pass `-o` to overwrite or `-n` to skip existing files.

### Loading Data

Use the lazy loader in `adlab.data`:

```python
from adlab.data import scan, downsample

# Lazy slice — nothing is read until .collect()
lf = scan('mission1', variant='z-normalized', split='test',
          t_start='2010-01-01', t_end='2010-02-01',
          columns=['channel_12'])
df = lf.collect()                    # polars DataFrame
pdf = df.to_pandas()                 # if you need pandas

# Downsampled overview for plotting big ranges
overview = downsample(scan('mission1', split='train'), bucket='1d').collect()
```

`scan()` returns a polars `LazyFrame` — filters and column selection are pushed down to the parquet reader, so time-range and column queries stay cheap even for the full dataset.
