"""
Read the Delta silver table and write summary tables to gold/delta/.

Do not read raw/ here. Gold is built only from silver.
"""

from pathlib import Path

import pandas as pd
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SILVER_DELTA_PATH = PROJECT_ROOT / "silver" / "delta" / "life_expectancy"
GOLD_DIR = PROJECT_ROOT / "gold" / "delta"


def load_countries() -> pd.DataFrame:
    silver = DeltaTable(str(SILVER_DELTA_PATH)).to_pyarrow_table().to_pandas()
    return silver.loc[~silver["is_aggregate"]].copy()


def build_latest(countries: pd.DataFrame) -> pd.DataFrame:
    latest_year = countries.groupby("country_code")["year"].transform("max")
    latest = countries.loc[countries["year"] == latest_year].copy()
    return latest[
        [
            "country_code",
            "country_name",
            "region",
            "income_group",
            "year",
            "value",
            "indicator_code",
            "indicator_name",
        ]
    ].sort_values("value", ascending=False)


def build_change_since_2000(countries: pd.DataFrame, latest: pd.DataFrame) -> pd.DataFrame:
    year_2000 = countries.loc[countries["year"] == 2000, ["country_code", "value"]]
    year_2000 = year_2000.rename(columns={"value": "value_2000"})

    change = latest.merge(year_2000, on="country_code", how="inner")
    change = change.rename(columns={"value": "value_latest", "year": "latest_year"})
    change["change_years"] = change["value_latest"] - change["value_2000"]
    return change[
        [
            "country_code",
            "country_name",
            "region",
            "income_group",
            "value_2000",
            "latest_year",
            "value_latest",
            "change_years",
        ]
    ].sort_values("change_years", ascending=False)


def build_region_year(countries: pd.DataFrame) -> pd.DataFrame:
    region_year = (
        countries.groupby(["region", "year"], as_index=False)["value"]
        .mean()
        .rename(columns={"value": "avg_life_expectancy"})
    )
    return region_year.sort_values(["region", "year"])


def main() -> None:
    GOLD_DIR.mkdir(exist_ok=True)

    countries = load_countries()
    latest = build_latest(countries)
    change = build_change_since_2000(countries, latest)
    region_year = build_region_year(countries)

    latest_path = GOLD_DIR / "life_expectancy_latest.parquet"
    change_path = GOLD_DIR / "life_expectancy_change_since_2000.parquet"
    region_path = GOLD_DIR / "life_expectancy_region_year.parquet"

    latest.to_parquet(latest_path, index=False)
    change.to_parquet(change_path, index=False)
    region_year.to_parquet(region_path, index=False)

    print(f"silver country rows used: {len(countries)}")
    print(f"gold latest (one row per country): {len(latest)} -> {latest_path.name}")
    print(f"gold change since 2000: {len(change)} -> {change_path.name}")
    print(f"gold region-year averages: {len(region_year)} -> {region_path.name}")


if __name__ == "__main__":
    main()
