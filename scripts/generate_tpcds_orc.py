#!/usr/bin/env python3
"""
Generate TPC-DS ORC files from TPC-DS tool output.

This script reads TPC-DS tool generated data files and converts them to ORC format.
It supports both direct conversion from text files and conversion from Parquet.

Usage:
    # From TPC-DS text files
    python3 generate_tpcds_orc.py --from-text <tpcds_data_dir> --output <orc_dir>

    # From Parquet files (if you have Parquet data)
    python3 generate_tpcds_orc.py --from-parquet <parquet_dir> --output <orc_dir>

Requirements:
    pip install pyarrow pandas
"""

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Optional

try:
    import pyarrow as pa
    import pyarrow.orc as orc
    import pyarrow.parquet as pq
    import pandas as pd
except ImportError:
    print("Error: Required packages not found.")
    print("Install with: pip install pyarrow pandas")
    sys.exit(1)

# TPC-DS table schemas (based on TPC-DS specification)
# These schemas define the column types for each table
TPCDS_SCHEMAS = {
    "call_center": {
        "cc_call_center_sk": "int32",
        "cc_call_center_id": "string",
        "cc_rec_start_date": "date32",
        "cc_rec_end_date": "date32",
        "cc_closed_date_sk": "int32",
        "cc_open_date_sk": "int32",
        "cc_name": "string",
        "cc_class": "string",
        "cc_employees": "int32",
        "cc_sq_ft": "int32",
        "cc_hours": "string",
        "cc_manager": "string",
        "cc_mkt_id": "int32",
        "cc_mkt_class": "string",
        "cc_mkt_desc": "string",
        "cc_market_manager": "string",
        "cc_division": "int32",
        "cc_division_name": "string",
        "cc_company": "int32",
        "cc_company_name": "string",
        "cc_street_number": "string",
        "cc_street_name": "string",
        "cc_street_type": "string",
        "cc_suite_number": "string",
        "cc_city": "string",
        "cc_county": "string",
        "cc_state": "string",
        "cc_zip": "string",
        "cc_country": "string",
        "cc_gmt_offset": "decimal128(5,2)",
        "cc_tax_percentage": "decimal128(5,2)",
    },
    # Add more table schemas as needed
    # For brevity, we'll handle common patterns programmatically
}


def parse_decimal_type(type_str: str) -> tuple:
    """Parse decimal type string like 'decimal128(7,2)' into (precision, scale)."""
    if "decimal" in type_str.lower():
        import re
        match = re.search(r"\((\d+),(\d+)\)", type_str)
        if match:
            return (int(match.group(1)), int(match.group(2)))
    return (10, 2)  # default


def infer_schema_from_parquet(parquet_path: Path) -> pa.Schema:
    """Read schema from Parquet file."""
    return pq.read_schema(parquet_path)


def read_tpcds_text_file(
    text_path: Path, delimiter: str = "|", null_value: str = ""
) -> pd.DataFrame:
    """
    Read TPC-DS text file (pipe-delimited by default).

    TPC-DS data files are typically pipe-delimited with empty strings for NULL.
    """
    # Read CSV with pipe delimiter
    df = pd.read_csv(
        text_path,
        delimiter=delimiter,
        quoting=csv.QUOTE_NONE,
        na_values=[null_value],
        keep_default_na=False,
        dtype=str,  # Read all as string first, then convert
        low_memory=False,
    )

    # Remove trailing empty columns (TPC-DS files often have trailing pipes)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

    return df


def convert_text_to_orc(
    text_path: Path,
    orc_path: Path,
    schema: Optional[pa.Schema] = None,
    delimiter: str = "|",
    verbose: bool = True,
) -> bool:
    """Convert TPC-DS text file to ORC format."""
    try:
        if verbose:
            print(f"Reading {text_path.name}...", end=" ", flush=True)

        # Read text file
        df = read_tpcds_text_file(text_path, delimiter=delimiter)

        if verbose:
            print(f"({len(df)} rows)", end=" ", flush=True)

        # Convert to Arrow table
        if schema:
            # Use provided schema for type conversion
            table = pa.Table.from_pandas(df, schema=schema)
        else:
            # Infer types (less accurate but works)
            table = pa.Table.from_pandas(df)

        # Write ORC file
        orc.write_table(table, orc_path)

        if verbose:
            orc_size = orc_path.stat().st_size
            print(f"→ {orc_path.name} ({orc_size:,} bytes) ✓")
        return True
    except Exception as e:
        if verbose:
            print(f"✗ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def convert_parquet_to_orc(
    parquet_path: Path, orc_path: Path, verbose: bool = True
) -> bool:
    """Convert Parquet file to ORC format."""
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


def find_table_files(data_dir: Path, extension: str) -> dict:
    """Find all table files in directory."""
    files = {}
    for file_path in data_dir.glob(f"*.{extension}"):
        # Extract table name (remove extension and any prefix like 'sf1_')
        table_name = file_path.stem
        # Remove scale factor prefix if present (e.g., 'sf1_customer' -> 'customer')
        if "_" in table_name:
            parts = table_name.split("_", 1)
            if parts[0].startswith("sf") and parts[0][2:].isdigit():
                table_name = parts[1]
        files[table_name] = file_path
    return files


def main():
    parser = argparse.ArgumentParser(
        description="Generate TPC-DS ORC files from TPC-DS tool output",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # From TPC-DS text files (pipe-delimited)
  python3 generate_tpcds_orc.py --from-text /path/to/tpcds/data --output /path/to/orc

  # From Parquet files
  python3 generate_tpcds_orc.py --from-parquet /path/to/parquet --output /path/to/orc

  # Custom delimiter for text files
  python3 generate_tpcds_orc.py --from-text /path/to/data --output /path/to/orc --delimiter "\\t"
        """,
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--from-text",
        type=str,
        help="Directory containing TPC-DS text files (*.dat or *.txt)",
    )
    input_group.add_argument(
        "--from-parquet",
        type=str,
        help="Directory containing Parquet files (*.parquet)",
    )

    parser.add_argument(
        "--output",
        type=str,
        required=True,
        help="Output directory for ORC files",
    )
    parser.add_argument(
        "--delimiter",
        type=str,
        default="|",
        help="Delimiter for text files (default: '|')",
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

    # Convert delimiter escape sequences
    delimiter = args.delimiter.encode().decode("unicode_escape")

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    success_count = 0
    failed_count = 0
    skipped_count = 0

    if args.from_text:
        # Convert from text files
        text_dir = Path(args.from_text)
        if not text_dir.exists():
            print(f"Error: Text directory does not exist: {text_dir}")
            sys.exit(1)

        # Find all text files
        text_files = {}
        for ext in ["dat", "txt"]:
            text_files.update(find_table_files(text_dir, ext))

        if not text_files:
            print(f"Error: No text files found in {text_dir}")
            sys.exit(1)

        if not args.quiet:
            print(f"Found {len(text_files)} text files in {text_dir}")
            print(f"Converting to ORC format in {output_dir}\n")

        for table_name, text_path in sorted(text_files.items()):
            orc_path = output_dir / f"{table_name}.orc"

            if args.skip_existing and orc_path.exists():
                if not args.quiet:
                    print(f"⊘ Skipping {table_name}.orc (already exists)")
                skipped_count += 1
                continue

            if convert_text_to_orc(
                text_path, orc_path, delimiter=delimiter, verbose=not args.quiet
            ):
                success_count += 1
            else:
                failed_count += 1

    elif args.from_parquet:
        # Convert from Parquet files
        parquet_dir = Path(args.from_parquet)
        if not parquet_dir.exists():
            print(f"Error: Parquet directory does not exist: {parquet_dir}")
            sys.exit(1)

        # Find all Parquet files
        parquet_files = find_table_files(parquet_dir, "parquet")

        if not parquet_files:
            print(f"Error: No Parquet files found in {parquet_dir}")
            sys.exit(1)

        if not args.quiet:
            print(f"Found {len(parquet_files)} Parquet files in {parquet_dir}")
            print(f"Converting to ORC format in {output_dir}\n")

        for table_name, parquet_path in sorted(parquet_files.items()):
            orc_path = output_dir / f"{table_name}.orc"

            if args.skip_existing and orc_path.exists():
                if not args.quiet:
                    print(f"⊘ Skipping {table_name}.orc (already exists)")
                skipped_count += 1
                continue

            if convert_parquet_to_orc(
                parquet_path, orc_path, verbose=not args.quiet
            ):
                success_count += 1
            else:
                failed_count += 1

    # Summary
    if not args.quiet:
        print(f"\n{'='*60}")
        print(f"Conversion Summary:")
        print(f"  ✓ Successfully converted: {success_count}")
        if skipped_count > 0:
            print(f"  ⊘ Skipped: {skipped_count}")
        if failed_count > 0:
            print(f"  ✗ Failed: {failed_count}")
        print(f"{'='*60}")

    if failed_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()

