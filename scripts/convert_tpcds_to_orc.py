#!/usr/bin/env python3
"""
Convert TPC-DS Parquet files to ORC format.

This script converts TPC-DS Parquet data files to ORC format for use with
the TPC-DS benchmark tool.

Usage:
    python3 convert_tpcds_to_orc.py <parquet_dir> <orc_dir>

Example:
    python3 convert_tpcds_to_orc.py \
        /path/to/datafusion-benchmarks/tpcds/data/sf1 \
        /tmp/tpcds_sf1_orc

Requirements:
    pip install pyarrow
"""

import os
import sys
import argparse
from pathlib import Path

try:
    import pyarrow.parquet as pq
    import pyarrow.orc as orc
except ImportError:
    print("Error: pyarrow is required. Install it with: pip install pyarrow")
    sys.exit(1)

# TPC-DS table names
TPCDS_TABLES = [
    "call_center",
    "customer_address",
    "household_demographics",
    "promotion",
    "store_sales",
    "web_page",
    "catalog_page",
    "customer_demographics",
    "income_band",
    "reason",
    "store",
    "web_returns",
    "catalog_returns",
    "customer",
    "inventory",
    "ship_mode",
    "time_dim",
    "web_sales",
    "catalog_sales",
    "date_dim",
    "item",
    "store_returns",
    "warehouse",
    "web_site",
]


def convert_table(parquet_path: Path, orc_path: Path, verbose: bool = True) -> bool:
    """Convert a single table from Parquet to ORC."""
    try:
        if verbose:
            print(f"Converting {parquet_path.name}...", end=" ", flush=True)

        # Read Parquet file
        table = pq.read_table(parquet_path)

        # Write ORC file
        orc.write_table(table, orc_path)

        if verbose:
            parquet_size = parquet_path.stat().st_size
            orc_size = orc_path.stat().st_size
            ratio = (1 - orc_size / parquet_size) * 100 if parquet_size > 0 else 0
            print(f"✓ ({orc_size:,} bytes, {ratio:+.1f}% vs Parquet)")
        return True
    except Exception as e:
        if verbose:
            print(f"✗ Error: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Convert TPC-DS Parquet files to ORC format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Convert from datafusion-benchmarks
  python3 convert_tpcds_to_orc.py \\
      ~/datafusion-benchmarks/tpcds/data/sf1 \\
      ~/tpcds_sf1_orc

  # Quiet mode (no progress output)
  python3 convert_tpcds_to_orc.py -q parquet_dir orc_dir
        """,
    )
    parser.add_argument(
        "parquet_dir",
        type=str,
        help="Directory containing Parquet files (*.parquet)",
    )
    parser.add_argument(
        "orc_dir",
        type=str,
        help="Output directory for ORC files (*.orc)",
    )
    parser.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        help="Quiet mode: suppress progress output",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip files that already exist in output directory",
    )

    args = parser.parse_args()

    parquet_dir = Path(args.parquet_dir)
    orc_dir = Path(args.orc_dir)

    # Validate input directory
    if not parquet_dir.exists():
        print(f"Error: Parquet directory does not exist: {parquet_dir}")
        sys.exit(1)

    if not parquet_dir.is_dir():
        print(f"Error: Not a directory: {parquet_dir}")
        sys.exit(1)

    # Create output directory
    orc_dir.mkdir(parents=True, exist_ok=True)

    # Convert tables
    success_count = 0
    skipped_count = 0
    failed_count = 0

    for table in TPCDS_TABLES:
        parquet_path = parquet_dir / f"{table}.parquet"
        orc_path = orc_dir / f"{table}.orc"

        # Check if source file exists
        if not parquet_path.exists():
            if not args.quiet:
                print(f"⚠ Warning: {parquet_path.name} does not exist, skipping")
            skipped_count += 1
            continue

        # Check if should skip existing
        if args.skip_existing and orc_path.exists():
            if not args.quiet:
                print(f"⊘ Skipping {table}.orc (already exists)")
            skipped_count += 1
            continue

        # Convert
        if convert_table(parquet_path, orc_path, verbose=not args.quiet):
            success_count += 1
        else:
            failed_count += 1

    # Summary
    if not args.quiet:
        print(f"\n{'='*60}")
        print(f"Conversion Summary:")
        print(f"  ✓ Successfully converted: {success_count}/{len(TPCDS_TABLES)}")
        if skipped_count > 0:
            print(f"  ⊘ Skipped: {skipped_count}")
        if failed_count > 0:
            print(f"  ✗ Failed: {failed_count}")
        print(f"{'='*60}")

    # Exit with error if any failed
    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

