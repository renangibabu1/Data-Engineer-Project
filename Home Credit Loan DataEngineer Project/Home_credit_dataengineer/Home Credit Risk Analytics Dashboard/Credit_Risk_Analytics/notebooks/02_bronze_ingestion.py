from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, functions as F


RAW_VOLUME_PATH = "/Volumes/credit_risk/bronze/raw_files"
BRONZE_SCHEMA = "credit_risk.bronze"
SOURCE_FILES = (
    ("application_train.csv", "application_train", None),
    ("application_test.csv", "application_test", None),
    ("bureau.csv", "bureau", None),
    ("bureau_balance.csv", "bureau_balance", None),
    ("credit_card_balance.csv", "credit_card_balance", None),
    ("installments_payments.csv", "installments_payments", None),
    ("POS_CASH_balance.csv", "pos_cash_balance", None),
    ("previous_application.csv", "previous_application", None),
    ("sample_submission.csv", "sample_submission", None),
    ("HomeCredit_columns_description.csv", "homecredit_columns_description", "cp1252"),
)


def read_source_file(
    spark: DatabricksSession,
    file_name: str,
    encoding: str | None,
) -> DataFrame:
    reader = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
    )
    if encoding is not None:
        reader = reader.option("encoding", encoding)

    source_path = f"{RAW_VOLUME_PATH}/{file_name}"
    source_df = reader.load(source_path)
    if not source_df.columns:
        raise ValueError(f"{file_name} has no columns")

    return (
        source_df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest_source_file(
    spark: DatabricksSession,
    file_name: str,
    table_name: str,
    encoding: str | None,
) -> None:
    target_table = f"{BRONZE_SCHEMA}.{table_name}"
    bronze_df = read_source_file(spark, file_name, encoding)
    source_row_count = bronze_df.count()
    if source_row_count == 0:
        raise ValueError(f"No data rows found in {RAW_VOLUME_PATH}/{file_name}")

    (
        bronze_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    bronze_row_count = spark.table(target_table).count()
    if bronze_row_count != source_row_count:
        raise RuntimeError(
            f"Bronze row-count mismatch for {file_name}: "
            f"source={source_row_count}, table={bronze_row_count}"
        )

    print(
        f"{file_name} -> {target_table}: "
        f"{bronze_row_count:,} rows verified"
    )


def main() -> None:
    spark = DatabricksSession.builder.serverless().getOrCreate()
    try:
        for file_name, table_name, encoding in SOURCE_FILES:
            ingest_source_file(spark, file_name, table_name, encoding)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
