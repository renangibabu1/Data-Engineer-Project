"""Validate Silver Delta and Gold Parquet after running both pipeline stages.

Run from the project root with: python scripts/03_validate_pipeline.py
"""

from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_DELTA_PATH = PROJECT_ROOT / "silver" / "delta" / "life_expectancy"
GOLD_DIR = PROJECT_ROOT / "gold" / "delta"

SILVER_COLUMNS = [
    "country_name",
    "country_code",
    "indicator_name",
    "indicator_code",
    "year",
    "value",
    "region",
    "income_group",
    "is_aggregate",
]
LATEST_COLUMNS = [
    "country_code",
    "country_name",
    "region",
    "income_group",
    "year",
    "value",
    "indicator_code",
    "indicator_name",
]
CHANGE_COLUMNS = [
    "country_code",
    "country_name",
    "region",
    "income_group",
    "value_2000",
    "latest_year",
    "value_latest",
    "change_years",
]
REGION_COLUMNS = ["region", "year", "avg_life_expectancy"]
VALUE_TOLERANCE = 1e-9


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def require_columns(frame: pd.DataFrame, expected: list[str], name: str) -> None:
    require(
        frame.columns.tolist() == expected,
        f"{name} has unexpected columns: {frame.columns.tolist()}",
    )


def require_close(actual: pd.Series, expected: pd.Series, message: str) -> None:
    require(
        (actual - expected).abs().le(VALUE_TOLERANCE).all(),
        message,
    )


def load_silver() -> pd.DataFrame:
    delta_log = SILVER_DELTA_PATH / "_delta_log"
    require(delta_log.is_dir(), f"Silver Delta table not found: {SILVER_DELTA_PATH}")

    silver = DeltaTable(str(SILVER_DELTA_PATH)).to_pyarrow_table().to_pandas()
    require_columns(silver, SILVER_COLUMNS, "Silver")
    require(not silver.empty, "Silver table is empty.")
    require(
        silver[["country_name", "country_code", "year", "value", "is_aggregate"]]
        .notna()
        .all()
        .all(),
        "Silver contains nulls in required fields.",
    )
    require(
        not silver.duplicated(["country_code", "year"]).any(),
        "Silver contains duplicate country-year records.",
    )
    require(
        silver["year"].between(1900, 2100).all(),
        "Silver contains years outside the supported range 1900-2100.",
    )
    require(
        silver["value"].between(0, 100).all(),
        "Silver contains life expectancy values outside the range 0-100.",
    )

    region_is_missing = silver["region"].isna() | silver["region"].astype(str).str.strip().eq("")
    require(
        silver["is_aggregate"].eq(region_is_missing).all(),
        "Silver aggregate flags do not match missing or blank regions.",
    )
    return silver


def validate_gold(silver: pd.DataFrame) -> dict[str, int]:
    latest_path = GOLD_DIR / "life_expectancy_latest.parquet"
    change_path = GOLD_DIR / "life_expectancy_change_since_2000.parquet"
    region_path = GOLD_DIR / "life_expectancy_region_year.parquet"
    for path in (latest_path, change_path, region_path):
        require(path.is_file(), f"Gold output not found: {path}")

    latest = pd.read_parquet(latest_path)
    change = pd.read_parquet(change_path)
    region = pd.read_parquet(region_path)
    require_columns(latest, LATEST_COLUMNS, "Gold latest")
    require_columns(change, CHANGE_COLUMNS, "Gold change since 2000")
    require_columns(region, REGION_COLUMNS, "Gold region-year")

    countries = silver.loc[~silver["is_aggregate"]].copy()
    require(not countries.empty, "Silver contains no country rows.")

    expected_latest = countries.loc[
        countries["year"].eq(countries.groupby("country_code")["year"].transform("max")),
        ["country_code", "year", "value"],
    ]
    require(
        latest["country_code"].is_unique,
        "Gold latest contains more than one row per country.",
    )
    require(
        set(latest["country_code"]) == set(expected_latest["country_code"]),
        "Gold latest country set does not match Silver.",
    )
    latest_check = latest[["country_code", "year", "value"]].merge(
        expected_latest,
        on="country_code",
        how="inner",
        suffixes=("_actual", "_expected"),
        validate="one_to_one",
    )
    require(
        latest_check["year_actual"].eq(latest_check["year_expected"]).all(),
        "Gold latest contains a year that is not the latest year in Silver.",
    )
    require_close(
        latest_check["value_actual"],
        latest_check["value_expected"],
        "Gold latest values do not match Silver.",
    )

    year_2000 = countries.loc[
        countries["year"].eq(2000), ["country_code", "value"]
    ].rename(columns={"value": "value_2000"})
    expected_change = latest[["country_code", "year", "value"]].rename(
        columns={"year": "latest_year", "value": "value_latest"}
    ).merge(year_2000, on="country_code", how="inner", validate="one_to_one")
    expected_change["change_years"] = (
        expected_change["value_latest"] - expected_change["value_2000"]
    )
    require(
        change["country_code"].is_unique,
        "Gold change table contains more than one row per country.",
    )
    require(
        set(change["country_code"]) == set(expected_change["country_code"]),
        "Gold change country set does not match countries with both latest and 2000 values.",
    )
    change_check = change[CHANGE_COLUMNS].merge(
        expected_change,
        on="country_code",
        how="inner",
        suffixes=("_actual", "_expected"),
        validate="one_to_one",
    )
    require(
        change_check["latest_year_actual"].eq(change_check["latest_year_expected"]).all(),
        "Gold change table has incorrect latest years.",
    )
    for column in ("value_2000", "value_latest", "change_years"):
        require_close(
            change_check[f"{column}_actual"],
            change_check[f"{column}_expected"],
            f"Gold change table has incorrect {column} values.",
        )

    expected_region = (
        countries.groupby(["region", "year"], as_index=False)["value"]
        .mean()
        .rename(columns={"value": "avg_life_expectancy"})
    )
    require(
        not region.duplicated(["region", "year"]).any(),
        "Gold region-year contains duplicate region-year records.",
    )
    require(
        len(region) == len(expected_region),
        "Gold region-year row count does not match Silver.",
    )
    region_check = region.merge(
        expected_region,
        on=["region", "year"],
        how="outer",
        suffixes=("_actual", "_expected"),
        indicator=True,
        validate="one_to_one",
    )
    require(
        region_check["_merge"].eq("both").all(),
        "Gold region-year keys do not match Silver.",
    )
    require_close(
        region_check["avg_life_expectancy_actual"],
        region_check["avg_life_expectancy_expected"],
        "Gold region-year averages do not match Silver.",
    )

    return {
        "latest": len(latest),
        "change_since_2000": len(change),
        "region_year": len(region),
    }


def main() -> None:
    silver = load_silver()
    gold_counts = validate_gold(silver)

    country_rows = int((~silver["is_aggregate"]).sum())
    aggregate_rows = len(silver) - country_rows
    print(
        f"PASS Silver: {len(silver):,} rows "
        f"({country_rows:,} countries, {aggregate_rows:,} aggregates), "
        f"years {silver['year'].min()}-{silver['year'].max()}"
    )
    print(
        "PASS Gold: "
        f"{gold_counts['latest']:,} latest, "
        f"{gold_counts['change_since_2000']:,} change-since-2000, "
        f"{gold_counts['region_year']:,} region-year rows"
    )
    print("Pipeline validation passed.")


if __name__ == "__main__":
    main()
