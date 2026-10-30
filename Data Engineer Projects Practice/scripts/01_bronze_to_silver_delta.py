"""Transform the World Bank CSVs into the Silver Delta table."""

import csv
import os
import sys
from pathlib import Path

from deltalake import write_deltalake
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "raw"
DATA_FILE = RAW_DIR / "API_SP.DYN.LE00.IN_DS2_en_csv_v2_1259.csv"
COUNTRY_FILE = RAW_DIR / "Metadata_Country_API_SP.DYN.LE00.IN_DS2_en_csv_v2_1259.csv"
SILVER_DELTA_PATH = PROJECT_ROOT / "silver" / "delta" / "life_expectancy"


def load_wide_data(spark: SparkSession):
    with DATA_FILE.open(encoding="utf-8-sig", newline="") as source:
        for _ in range(4):
            next(source, None)
        reader = csv.DictReader(source)
        if reader.fieldnames is None:
            raise ValueError("World Bank CSV has no header after its four preamble rows.")
        columns = [column for column in reader.fieldnames if column]
        rows = [tuple(row.get(column) for column in columns) for row in reader]

    if not rows:
        raise ValueError(f"World Bank CSV contains no data rows: {DATA_FILE}")
    return spark.createDataFrame(rows, schema=columns)


def build_silver(spark: SparkSession):
    wide = load_wide_data(spark)
    metadata = (
        spark.read.option("header", "true")
        .option("inferSchema", "false")
        .csv(str(COUNTRY_FILE))
    )

    year_columns = [column for column in wide.columns if column.isdigit()]
    if not year_columns:
        raise ValueError("No year columns found in the World Bank CSV.")

    year_values = F.array(
        *[
            F.struct(
                F.lit(int(year)).alias("year"),
                F.when(
                    F.trim(F.col(year)) != "",
                    F.col(year).cast("double"),
                )
                .otherwise(F.lit(None).cast("double"))
                .alias("value"),
            )
            for year in year_columns
        ]
    )
    long_data = (
        wide.select(
            "Country Name",
            "Country Code",
            "Indicator Name",
            "Indicator Code",
            F.explode(year_values).alias("year_value"),
        )
        .select(
            F.col("Country Name").alias("country_name"),
            F.col("Country Code").alias("country_code"),
            F.col("Indicator Name").alias("indicator_name"),
            F.col("Indicator Code").alias("indicator_code"),
            F.col("year_value.year").cast("integer").alias("year"),
            F.col("year_value.value").alias("value"),
        )
        .dropna(subset=["value"])
    )
    country_metadata = metadata.select(
        F.col("Country Code").alias("country_code"),
        F.col("Region").alias("region"),
        F.col("IncomeGroup").alias("income_group"),
    )
    return (
        long_data.join(country_metadata, on="country_code", how="left")
        .withColumn(
            "is_aggregate",
            F.col("region").isNull() | (F.trim(F.col("region")) == ""),
        )
        .select(
            "country_name",
            "country_code",
            "indicator_name",
            "indicator_code",
            "year",
            "value",
            "region",
            "income_group",
            "is_aggregate",
        )
    )


def main() -> None:
    for input_file in (DATA_FILE, COUNTRY_FILE):
        if not input_file.is_file():
            raise FileNotFoundError(f"Required source file not found: {input_file}")

    if os.name == "nt":
        os.environ["PYSPARK_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder.appName("GlobalHealthBronzeToSilver")
        .master("local[*]")
        .getOrCreate()
    )
    try:
        silver = build_silver(spark)
        row_count = silver.count()
        if row_count == 0:
            raise ValueError("Silver transformation produced no rows.")

        SILVER_DELTA_PATH.parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(
            str(SILVER_DELTA_PATH),
            silver.toArrow(),
            mode="overwrite",
        )
        country_count = silver.filter(~F.col("is_aggregate")).count()
        year_bounds = silver.agg(
            F.min("year").alias("min_year"),
            F.max("year").alias("max_year"),
        ).first()
        print(
            f"Silver Delta: {row_count:,} rows "
            f"({country_count:,} country rows), "
            f"years {year_bounds.min_year}-{year_bounds.max_year}"
        )
        print(f"Wrote: {SILVER_DELTA_PATH}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
