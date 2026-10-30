"""
Read World Bank files from raw/ and write a cleaned table to silver/.

Do not change files in raw/. That folder is the landing zone only.
"""

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "raw"
SILVER_DIR = PROJECT_ROOT / "silver"

DATA_FILE = RAW_DIR / "API_SP.DYN.LE00.IN_DS2_en_csv_v2_1259.csv"
COUNTRY_FILE = RAW_DIR / "Metadata_Country_API_SP.DYN.LE00.IN_DS2_en_csv_v2_1259.csv"
OUT_FILE = SILVER_DIR / "life_expectancy.parquet"


def load_raw_wide() -> pd.DataFrame:
    # World Bank CSV has 4 extra rows above the real header.
    return pd.read_csv(DATA_FILE, skiprows=4)


def melt_to_long(wide: pd.DataFrame) -> pd.DataFrame:
    id_cols = ["Country Name", "Country Code", "Indicator Name", "Indicator Code"]
    year_cols = [c for c in wide.columns if str(c).isdigit()]

    long_df = wide.melt(
        id_vars=id_cols,
        value_vars=year_cols,
        var_name="year",
        value_name="value",
    )
    long_df = long_df.rename(
        columns={
            "Country Name": "country_name",
            "Country Code": "country_code",
            "Indicator Name": "indicator_name",
            "Indicator Code": "indicator_code",
        }
    )
    long_df["year"] = long_df["year"].astype(int)
    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    return long_df.dropna(subset=["value"])


def add_country_metadata(facts: pd.DataFrame) -> pd.DataFrame:
    countries = pd.read_csv(COUNTRY_FILE)
    countries = countries.rename(
        columns={
            "Country Code": "country_code",
            "Region": "region",
            "IncomeGroup": "income_group",
            "TableName": "table_name",
        }
    )[["country_code", "region", "income_group"]]

    merged = facts.merge(countries, on="country_code", how="left")
    # Rows with a region are countries. Empty region = World Bank aggregates (AFE, ARB, ...).
    merged["is_aggregate"] = merged["region"].isna() | (merged["region"].astype(str).str.strip() == "")
    return merged


def main() -> None:
    SILVER_DIR.mkdir(exist_ok=True)

    wide = load_raw_wide()
    long_df = melt_to_long(wide)
    silver = add_country_metadata(long_df)

    silver.to_parquet(OUT_FILE, index=False)

    countries_only = silver.loc[~silver["is_aggregate"]]
    print(f"raw rows (wide countries/aggregates): {len(wide)}")
    print(f"silver rows (country-year with a value): {len(silver)}")
    print(f"country rows only (no aggregates): {len(countries_only)}")
    print(f"year range: {silver['year'].min()} - {silver['year'].max()}")
    print(f"wrote: {OUT_FILE}")


if __name__ == "__main__":
    main()
