import argparse
import json
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

import pandas as pd


DEFAULT_DATA_DIR = Path.home() / "Downloads" / "home-credit-default-risk"
DEFAULT_REPORT = Path(__file__).resolve().parents[1] / "reports" / "data_profile.json"
KEY_COLUMNS = {
    "application_test.csv": ("SK_ID_CURR",),
    "application_train.csv": ("SK_ID_CURR",),
    "bureau.csv": ("SK_ID_BUREAU",),
    "bureau_balance.csv": ("SK_ID_BUREAU", "MONTHS_BALANCE"),
    "credit_card_balance.csv": ("SK_ID_PREV", "MONTHS_BALANCE"),
    "installments_payments.csv": (
        "SK_ID_PREV",
        "NUM_INSTALMENT_NUMBER",
        "NUM_INSTALMENT_VERSION",
    ),
    "pos_cash_balance.csv": ("SK_ID_PREV", "MONTHS_BALANCE"),
    "previous_application.csv": ("SK_ID_PREV",),
    "sample_submission.csv": ("SK_ID_CURR",),
}


def profile_csv(
    path: Path,
    connection: sqlite3.Connection,
    chunk_size: int,
    max_rows: int | None,
) -> dict:
    encoding = (
        "cp1252"
        if path.name.lower() == "homecredit_columns_description.csv"
        else "utf-8-sig"
    )
    columns = pd.read_csv(path, nrows=0, encoding=encoding).columns.tolist()
    candidate_keys = KEY_COLUMNS.get(path.name.lower(), ())
    keys = [column for column in candidate_keys if column in columns]
    key_columns_found = len(keys) == len(candidate_keys) and bool(candidate_keys)

    connection.execute("DROP TABLE IF EXISTS seen_keys")
    if key_columns_found:
        key_fields = ", ".join(f"k{index} TEXT NOT NULL" for index in range(len(keys)))
        primary_key = ", ".join(f"k{index}" for index in range(len(keys)))
        connection.execute(
            f"CREATE TABLE seen_keys ({key_fields}, PRIMARY KEY ({primary_key}))"
        )

    missing_values = {column: 0 for column in columns}
    row_count = 0
    duplicate_key_rows = 0
    first_chunk_dtypes = {}
    scan_complete = True
    dtype_overrides = {key: "string" for key in keys}

    reader = pd.read_csv(
        path,
        chunksize=chunk_size,
        encoding=encoding,
        dtype=dtype_overrides,
    )
    try:
        for chunk in reader:
            if max_rows is not None:
                remaining = max_rows - row_count
                if remaining <= 0:
                    scan_complete = False
                    break
                chunk = chunk.iloc[:remaining]

            if chunk.empty:
                break

            if not first_chunk_dtypes:
                first_chunk_dtypes = {
                    column: str(dtype) for column, dtype in chunk.dtypes.items()
                }

            row_count += len(chunk)
            for column, count in chunk.isna().sum().items():
                missing_values[column] += int(count)

            if key_columns_found:
                key_rows = chunk[keys].dropna()
                records = [
                    tuple(str(value) for value in row)
                    for row in key_rows.itertuples(index=False, name=None)
                ]
                before = connection.total_changes
                connection.executemany(
                    f"INSERT OR IGNORE INTO seen_keys VALUES "
                    f"({', '.join('?' for _ in keys)})",
                    records,
                )
                duplicate_key_rows += len(records) - (
                    connection.total_changes - before
                )
                connection.commit()

            if max_rows is not None and row_count >= max_rows:
                scan_complete = False
                break
    finally:
        reader.close()

    return {
        "file": path.name,
        "size_bytes": path.stat().st_size,
        "encoding": encoding,
        "columns": columns,
        "column_count": len(columns),
        "rows_scanned": row_count,
        "scan_complete": scan_complete,
        "dtypes_first_chunk": first_chunk_dtypes,
        "missing_values": missing_values,
        "candidate_key_columns": list(candidate_keys),
        "candidate_key_columns_found": keys,
        "duplicate_candidate_key_rows": (
            duplicate_key_rows if key_columns_found else None
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Profile the Home Credit CSV files using bounded-memory chunks."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Folder containing the original CSV files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_REPORT,
        help="Path for the JSON profile report.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        help="Rows per Pandas chunk (default: 100000).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        help="Optional per-file row limit for a quick, partial profile.",
    )
    args = parser.parse_args()

    if args.chunk_size <= 0:
        parser.error("--chunk-size must be greater than zero")
    if args.max_rows is not None and args.max_rows <= 0:
        parser.error("--max-rows must be greater than zero")
    if not args.data_dir.is_dir():
        parser.error(f"CSV folder does not exist: {args.data_dir}")

    csv_files = sorted(args.data_dir.glob("*.csv"))
    if not csv_files:
        parser.error(f"No CSV files found in: {args.data_dir}")

    with tempfile.TemporaryDirectory(prefix="credit_risk_profile_") as temp_dir:
        database_path = Path(temp_dir) / "candidate_keys.sqlite"
        with closing(sqlite3.connect(database_path)) as connection:
            profiles = [
                profile_csv(path, connection, args.chunk_size, args.max_rows)
                for path in csv_files
            ]

    report = {
        "data_directory": str(args.data_dir.resolve()),
        "chunk_size": args.chunk_size,
        "row_limit_per_file": args.max_rows,
        "files": profiles,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")

    for profile in profiles:
        missing = sorted(
            (
                (column, count)
                for column, count in profile["missing_values"].items()
                if count
            ),
            key=lambda item: item[1],
            reverse=True,
        )[:5]
        print(
            f"{profile['file']}: {profile['rows_scanned']:,} rows scanned, "
            f"{profile['column_count']} columns, "
            f"{profile['size_bytes'] / (1024 ** 2):.1f} MiB, "
            f"scan complete={profile['scan_complete']}, "
            f"duplicate candidate-key rows="
            f"{profile['duplicate_candidate_key_rows']}"
        )
        if missing:
            print(
                "  Most-missing columns:",
                ", ".join(f"{column}={count:,}" for column, count in missing),
            )
    print(f"Profile report saved to: {args.output.resolve()}")


if __name__ == "__main__":
    main()