import pandas as pd
from pathlib import Path
import sys

# Find latest parquet file
data_dir = Path('data')
parquet_files = list(data_dir.glob('*.parquet'))

if not parquet_files:
    print("No parquet files found")
    sys.exit(1)

latest = max(parquet_files, key=lambda p: p.stat().st_mtime)
print(f"Converting: {latest.name}")

# Read and convert
df = pd.read_parquet(latest)
print(f"Rows: {len(df)}")

# Write CSV
output_path = latest.with_suffix('.csv')
df.to_csv(output_path, index=False)

print(f"CSV created: {output_path.name}")
print(f"Size: {output_path.stat().st_size / 1024:.2f} KB")
