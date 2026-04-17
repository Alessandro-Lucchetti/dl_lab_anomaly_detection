# ESA-ADB Processed Datasets

This directory contains the European Space Agency Anomaly Detection Benchmark (ESA-ADB) datasets, processed into Parquet format and partitioned by year and month for efficient access.

## Data Origin
The raw data originates from the [ESA-ADB Zenodo repository](https://doi.org/10.5281/zenodo.12528696). 
As detailed in the ESA-ADB paper (Section 2.3), the telemetry data was **already normalized to a 0-to-1 range** globally per channel group by ESA as part of an irreversible anonymization process.

The datasets in this folder were generated from the CSV files produced by the original ESA preprocessing scripts (`Mission*_semisupervised_prep_from_raw.py`). These scripts perform:
1. **Resampling**: Zero-order hold (forward fill) at 0.033 Hz (Mission 1) and 0.056 Hz (Mission 2).
2. **Derivatives**: First-order difference (`np.diff`) for monotonic channels (counters, etc.).

## Dataset Versions
We provide two versions of the datasets:

1. **`raw`**: The resampled data exactly as produced by the ESA scripts (already 0-1 scaled by ESA).
2. **`z-normalized`**: Data obtained by applying **local per-channel z-score normalization** `(x - mean) / std`.
   - **Important**: To prevent data leakage, the mean and standard deviation were computed **strictly on the training sets** (84 months for Mission 1, 21 months for Mission 2) and then applied to the entire timeline.

## Directory Structure
The data is partitioned by Year and Month into Parquet files:
```
data/processed/
├── mission1/
│   ├── raw/
│   │   ├── 2000_01.parquet
│   │   └── ...
│   └── z-normalized/
└── mission2/
    ├── raw/
    └── z-normalized/
```

## How to use
You can read the files using `pandas` or `polars`:

### Pandas
```python
import pandas as pd
df = pd.read_parquet("data/processed/mission1/z-normalized/2000_01.parquet")
```

### Polars
```python
import polars as pl
df = pl.read_parquet("data/processed/mission1/z-normalized/2000_01.parquet")
```

## Generation Script
The script used to generate these datasets is `_scripts/generate_datasets.py`.
