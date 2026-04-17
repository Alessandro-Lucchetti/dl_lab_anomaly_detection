import pandas as pd
from tqdm import tqdm
import sys
from adlab import ROOT


def process_mission(mission_id, train_csv, test_csv, output_base):
    print(f"\n--- Processing {mission_id} ---")

    print(f"Reading train data: {train_csv.name}...")
    train_df = pd.read_csv(train_csv)
    train_df['timestamp'] = pd.to_datetime(train_df['timestamp'])

    print(f"Reading test data: {test_csv.name}...")
    test_df = pd.read_csv(test_csv)
    test_df['timestamp'] = pd.to_datetime(test_df['timestamp'])

    feature_cols = [c for c in train_df.columns
                    if (c.startswith('channel_') or c.startswith('telecommand_'))
                    and not c.startswith('is_anomaly_')]

    # Z-normalization: stats from train only (avoid leakage). Vectorized assignment
    # prevents DataFrame fragmentation.
    print("Z-normalizing (train stats)...")
    means = train_df[feature_cols].mean()
    stds = train_df[feature_cols].std().replace(0, 1.0)

    z_train_df = train_df.copy()
    z_test_df = test_df.copy()
    z_train_df[feature_cols] = (train_df[feature_cols] - means) / stds
    z_test_df[feature_cols] = (test_df[feature_cols] - means) / stds

    variants = [
        ('raw', train_df, test_df),
        ('z-normalized', z_train_df, z_test_df),
    ]
    for variant, tr, te in variants:
        save_partitioned(tr, output_base / mission_id / variant / 'train')
        save_partitioned(te, output_base / mission_id / variant / 'test')


def save_partitioned(df, path):
    path.mkdir(parents=True, exist_ok=True)
    year_month = df['timestamp'].dt.strftime('%Y_%m')
    for ym, group in tqdm(df.groupby(year_month, sort=True),
                          desc=f"Saving to {path.relative_to(ROOT)}"):
        group.to_parquet(path / f"{ym}.parquet", index=False, engine='pyarrow')


if __name__ == "__main__":
    base_data = ROOT / "esa-adb/data/preprocessed/multivariate"
    output_base = ROOT / "data"

    try:
        m1_dir = base_data / "ESA-Mission1-semi-supervised"
        process_mission("mission1", m1_dir / "84_months.train.csv", m1_dir / "84_months.test.csv", output_base)

        m2_dir = base_data / "ESA-Mission2-semi-supervised"
        process_mission("mission2", m2_dir / "21_months.train.csv", m2_dir / "21_months.test.csv", output_base)

        print("\nAll datasets generated successfully.")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the ESA preprocessed CSV files are present in 'esa-adb/data/preprocessed/multivariate/'.")
        sys.exit(1)
