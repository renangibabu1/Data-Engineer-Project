from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, functions as F


SOURCE_TABLE = "credit_risk.bronze.application_train"
TARGET_TABLE = "credit_risk.silver.application_train"
METADATA_COLUMNS = {"_source_file", "_ingested_at"}

INTEGER_COLUMNS = frozenset(
    """
    SK_ID_CURR TARGET CNT_CHILDREN DAYS_BIRTH DAYS_EMPLOYED DAYS_ID_PUBLISH
    FLAG_MOBIL FLAG_EMP_PHONE FLAG_WORK_PHONE FLAG_CONT_MOBILE FLAG_PHONE
    FLAG_EMAIL REGION_RATING_CLIENT REGION_RATING_CLIENT_W_CITY
    HOUR_APPR_PROCESS_START REG_REGION_NOT_LIVE_REGION
    REG_REGION_NOT_WORK_REGION LIVE_REGION_NOT_WORK_REGION
    REG_CITY_NOT_LIVE_CITY REG_CITY_NOT_WORK_CITY LIVE_CITY_NOT_WORK_CITY
    FLAG_DOCUMENT_2 FLAG_DOCUMENT_3 FLAG_DOCUMENT_4 FLAG_DOCUMENT_5
    FLAG_DOCUMENT_6 FLAG_DOCUMENT_7 FLAG_DOCUMENT_8 FLAG_DOCUMENT_9
    FLAG_DOCUMENT_10 FLAG_DOCUMENT_11 FLAG_DOCUMENT_12 FLAG_DOCUMENT_13
    FLAG_DOCUMENT_14 FLAG_DOCUMENT_15 FLAG_DOCUMENT_16 FLAG_DOCUMENT_17
    FLAG_DOCUMENT_18 FLAG_DOCUMENT_19 FLAG_DOCUMENT_20 FLAG_DOCUMENT_21
    """.split()
)
DOUBLE_COLUMNS = frozenset(
    """
    AMT_INCOME_TOTAL AMT_CREDIT AMT_ANNUITY AMT_GOODS_PRICE
    REGION_POPULATION_RELATIVE DAYS_REGISTRATION OWN_CAR_AGE CNT_FAM_MEMBERS
    EXT_SOURCE_1 EXT_SOURCE_2 EXT_SOURCE_3 TOTALAREA_MODE
    OBS_30_CNT_SOCIAL_CIRCLE DEF_30_CNT_SOCIAL_CIRCLE
    OBS_60_CNT_SOCIAL_CIRCLE DEF_60_CNT_SOCIAL_CIRCLE
    DAYS_LAST_PHONE_CHANGE AMT_REQ_CREDIT_BUREAU_HOUR
    AMT_REQ_CREDIT_BUREAU_DAY AMT_REQ_CREDIT_BUREAU_WEEK
    AMT_REQ_CREDIT_BUREAU_MON AMT_REQ_CREDIT_BUREAU_QRT
    AMT_REQ_CREDIT_BUREAU_YEAR
    """.split()
) | frozenset(
    f"{feature}_{suffix}"
    for feature in (
        "APARTMENTS",
        "BASEMENTAREA",
        "YEARS_BEGINEXPLUATATION",
        "YEARS_BUILD",
        "COMMONAREA",
        "ELEVATORS",
        "ENTRANCES",
        "FLOORSMAX",
        "FLOORSMIN",
        "LANDAREA",
        "LIVINGAPARTMENTS",
        "LIVINGAREA",
        "NONLIVINGAPARTMENTS",
        "NONLIVINGAREA",
    )
    for suffix in ("AVG", "MODE", "MEDI")
)
STRING_COLUMNS = frozenset(
    """
    NAME_CONTRACT_TYPE CODE_GENDER FLAG_OWN_CAR FLAG_OWN_REALTY
    NAME_TYPE_SUITE NAME_INCOME_TYPE NAME_EDUCATION_TYPE NAME_FAMILY_STATUS
    NAME_HOUSING_TYPE OCCUPATION_TYPE WEEKDAY_APPR_PROCESS_START
    ORGANIZATION_TYPE FONDKAPREMONT_MODE HOUSETYPE_MODE WALLSMATERIAL_MODE
    EMERGENCYSTATE_MODE
    """.split()
)
EXPECTED_DATA_COLUMNS = INTEGER_COLUMNS | DOUBLE_COLUMNS | STRING_COLUMNS
NUMERIC_SQL_TYPES = {
    **dict.fromkeys(INTEGER_COLUMNS, "INT"),
    **dict.fromkeys(DOUBLE_COLUMNS, "DOUBLE"),
}
BUREAU_INTEGER_COLUMNS = frozenset(
    """
    SK_ID_CURR SK_ID_BUREAU DAYS_CREDIT CREDIT_DAY_OVERDUE
    CNT_CREDIT_PROLONG DAYS_CREDIT_UPDATE
    """.split()
)
BUREAU_DOUBLE_COLUMNS = frozenset(
    """
    DAYS_CREDIT_ENDDATE DAYS_ENDDATE_FACT AMT_CREDIT_MAX_OVERDUE
    AMT_CREDIT_SUM AMT_CREDIT_SUM_DEBT AMT_CREDIT_SUM_LIMIT
    AMT_CREDIT_SUM_OVERDUE AMT_ANNUITY
    """.split()
)
BUREAU_STRING_COLUMNS = frozenset(
    "CREDIT_ACTIVE CREDIT_CURRENCY CREDIT_TYPE".split()
)
BUREAU_EXPECTED_DATA_COLUMNS = (
    BUREAU_INTEGER_COLUMNS | BUREAU_DOUBLE_COLUMNS | BUREAU_STRING_COLUMNS
)
BUREAU_NUMERIC_SQL_TYPES = {
    **dict.fromkeys(BUREAU_INTEGER_COLUMNS, "INT"),
    **dict.fromkeys(BUREAU_DOUBLE_COLUMNS, "DOUBLE"),
}
PREVIOUS_APPLICATION_INTEGER_COLUMNS = frozenset(
    """
    SK_ID_PREV SK_ID_CURR HOUR_APPR_PROCESS_START NFLAG_LAST_APPL_IN_DAY
    DAYS_DECISION SELLERPLACE_AREA
    """.split()
)
PREVIOUS_APPLICATION_DOUBLE_COLUMNS = frozenset(
    """
    AMT_ANNUITY AMT_APPLICATION AMT_CREDIT AMT_DOWN_PAYMENT
    AMT_GOODS_PRICE RATE_DOWN_PAYMENT RATE_INTEREST_PRIMARY
    RATE_INTEREST_PRIVILEGED CNT_PAYMENT DAYS_FIRST_DRAWING
    DAYS_FIRST_DUE DAYS_LAST_DUE_1ST_VERSION DAYS_LAST_DUE
    DAYS_TERMINATION NFLAG_INSURED_ON_APPROVAL
    """.split()
)
PREVIOUS_APPLICATION_STRING_COLUMNS = frozenset(
    """
    NAME_CONTRACT_TYPE WEEKDAY_APPR_PROCESS_START FLAG_LAST_APPL_PER_CONTRACT
    NAME_CASH_LOAN_PURPOSE NAME_CONTRACT_STATUS NAME_PAYMENT_TYPE
    CODE_REJECT_REASON NAME_TYPE_SUITE NAME_CLIENT_TYPE NAME_GOODS_CATEGORY
    NAME_PORTFOLIO NAME_PRODUCT_TYPE CHANNEL_TYPE NAME_SELLER_INDUSTRY
    NAME_YIELD_GROUP PRODUCT_COMBINATION
    """.split()
)
PREVIOUS_APPLICATION_EXPECTED_DATA_COLUMNS = (
    PREVIOUS_APPLICATION_INTEGER_COLUMNS
    | PREVIOUS_APPLICATION_DOUBLE_COLUMNS
    | PREVIOUS_APPLICATION_STRING_COLUMNS
)
PREVIOUS_APPLICATION_NUMERIC_SQL_TYPES = {
    **dict.fromkeys(PREVIOUS_APPLICATION_INTEGER_COLUMNS, "INT"),
    **dict.fromkeys(PREVIOUS_APPLICATION_DOUBLE_COLUMNS, "DOUBLE"),
}
INSTALLMENT_INTEGER_COLUMNS = frozenset(
    "SK_ID_PREV SK_ID_CURR NUM_INSTALMENT_NUMBER".split()
)
INSTALLMENT_DOUBLE_COLUMNS = frozenset(
    """
    NUM_INSTALMENT_VERSION DAYS_INSTALMENT DAYS_ENTRY_PAYMENT
    AMT_INSTALMENT AMT_PAYMENT
    """.split()
)
INSTALLMENT_EXPECTED_DATA_COLUMNS = (
    INSTALLMENT_INTEGER_COLUMNS | INSTALLMENT_DOUBLE_COLUMNS
)
INSTALLMENT_NUMERIC_SQL_TYPES = {
    **dict.fromkeys(INSTALLMENT_INTEGER_COLUMNS, "INT"),
    **dict.fromkeys(INSTALLMENT_DOUBLE_COLUMNS, "DOUBLE"),
}


def _validate_columns(
    bronze_df: DataFrame,
    expected_data_columns: frozenset[str],
    table_name: str,
) -> None:
    actual_columns = set(bronze_df.columns)
    missing_metadata = METADATA_COLUMNS - actual_columns
    if missing_metadata:
        raise ValueError(
            f"Bronze table is missing metadata columns: {sorted(missing_metadata)}"
        )

    actual_data_columns = actual_columns - METADATA_COLUMNS
    missing_columns = expected_data_columns - actual_data_columns
    unexpected_columns = actual_data_columns - expected_data_columns
    if missing_columns or unexpected_columns:
        raise ValueError(
            f"{table_name} schema does not match the verified source schema: "
            f"missing={sorted(missing_columns)}, "
            f"unexpected={sorted(unexpected_columns)}"
        )


def _cast_failure_counts(
    bronze_df: DataFrame,
    numeric_sql_types: dict[str, str],
) -> dict[str, int]:
    failure_expressions = []
    for column_name, sql_type in numeric_sql_types.items():
        raw_value = F.col(column_name)
        trimmed_value = F.trim(raw_value)
        parsed_value = F.expr(
            f"try_cast(NULLIF(TRIM(`{column_name}`), '') AS {sql_type})"
        )
        is_invalid = (
            raw_value.isNotNull()
            & (trimmed_value != "")
            & parsed_value.isNull()
        )
        failure_expressions.append(
            F.sum(F.when(is_invalid, 1).otherwise(0)).alias(column_name)
        )

    result = bronze_df.agg(*failure_expressions).first().asDict()
    return {
        column_name: int(count or 0)
        for column_name, count in result.items()
        if count
    }


def _build_silver_dataframe(
    bronze_df: DataFrame,
    numeric_sql_types: dict[str, str],
) -> DataFrame:
    expressions = []
    for column_name in bronze_df.columns:
        if column_name in METADATA_COLUMNS:
            continue
        if column_name in numeric_sql_types:
            expressions.append(
                F.expr(
                    f"try_cast(NULLIF(TRIM(`{column_name}`), '') "
                    f"AS {numeric_sql_types[column_name]})"
                ).alias(column_name)
            )
        else:
            trimmed_value = F.trim(F.col(column_name))
            expressions.append(
                F.when(trimmed_value == "", F.lit(None).cast("string"))
                .otherwise(trimmed_value)
                .alias(column_name)
            )

    expressions.extend(
        [F.col("_source_file"), F.col("_ingested_at")]
    )
    return bronze_df.select(*expressions)


def transform_application_train(spark: DatabricksSession) -> None:
    bronze_df = spark.table(SOURCE_TABLE)
    _validate_columns(
        bronze_df, EXPECTED_DATA_COLUMNS, "application_train"
    )

    cast_failures = _cast_failure_counts(bronze_df, NUMERIC_SQL_TYPES)
    if cast_failures:
        raise ValueError(
            f"Invalid numeric values found in Bronze: {cast_failures}"
        )

    silver_df = _build_silver_dataframe(bronze_df, NUMERIC_SQL_TYPES)
    bronze_row_count = bronze_df.count()
    silver_row_count = silver_df.count()
    if silver_row_count != bronze_row_count:
        raise RuntimeError(
            f"Silver transformation changed row count: "
            f"bronze={bronze_row_count}, silver={silver_row_count}"
        )

    null_key_count = silver_df.where(F.col("SK_ID_CURR").isNull()).count()
    duplicate_key_count = (
        silver_df.groupBy("SK_ID_CURR")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    invalid_target_count = silver_df.where(
        F.col("TARGET").isNull() | ~F.col("TARGET").isin(0, 1)
    ).count()
    if null_key_count or duplicate_key_count or invalid_target_count:
        raise ValueError(
            "Silver quality checks failed: "
            f"null_keys={null_key_count}, "
            f"duplicate_keys={duplicate_key_count}, "
            f"invalid_targets={invalid_target_count}"
        )

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(TARGET_TABLE)
    )

    written_row_count = spark.table(TARGET_TABLE).count()
    if written_row_count != bronze_row_count:
        raise RuntimeError(
            f"Silver row-count mismatch: bronze={bronze_row_count}, "
            f"table={written_row_count}"
        )

    print(f"Silver table: {TARGET_TABLE}")
    print(f"Rows written and verified: {written_row_count:,}")
    print(f"Columns checked: {len(EXPECTED_DATA_COLUMNS)} source columns")
    print(f"Null keys: {null_key_count}; duplicate keys: {duplicate_key_count}")
    print(f"Invalid target values: {invalid_target_count}")


def transform_bureau(spark: DatabricksSession) -> None:
    bronze_df = spark.table("credit_risk.bronze.bureau")
    _validate_columns(
        bronze_df, BUREAU_EXPECTED_DATA_COLUMNS, "bureau"
    )

    cast_failures = _cast_failure_counts(
        bronze_df, BUREAU_NUMERIC_SQL_TYPES
    )
    if cast_failures:
        raise ValueError(
            f"Invalid numeric values found in Bronze bureau: {cast_failures}"
        )

    silver_df = _build_silver_dataframe(
        bronze_df, BUREAU_NUMERIC_SQL_TYPES
    )
    bronze_row_count = bronze_df.count()
    silver_row_count = silver_df.count()
    if silver_row_count != bronze_row_count:
        raise RuntimeError(
            "Bureau Silver transformation changed row count: "
            f"bronze={bronze_row_count}, silver={silver_row_count}"
        )

    null_key_count = silver_df.where(
        F.col("SK_ID_CURR").isNull() | F.col("SK_ID_BUREAU").isNull()
    ).count()
    duplicate_key_count = (
        silver_df.groupBy("SK_ID_BUREAU")
        .count()
        .where(F.col("count") > 1)
        .count()
    )
    if null_key_count or duplicate_key_count:
        raise ValueError(
            "Bureau Silver quality checks failed: "
            f"null_keys={null_key_count}, "
            f"duplicate_SK_ID_BUREAU={duplicate_key_count}"
        )

    target_table = "credit_risk.silver.bureau"
    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    written_row_count = spark.table(target_table).count()
    if written_row_count != bronze_row_count:
        raise RuntimeError(
            f"Bureau Silver row-count mismatch: bronze={bronze_row_count}, "
            f"table={written_row_count}"
        )

    print(f"Silver table: {target_table}")
    print(f"Rows written and verified: {written_row_count:,}")
    print(f"Columns checked: {len(BUREAU_EXPECTED_DATA_COLUMNS)} source columns")
    print(
        "Null bureau/customer keys: "
        f"{null_key_count}; duplicate bureau keys: {duplicate_key_count}"
    )


def _transform_related_table(
    spark: DatabricksSession,
    source_table: str,
    target_table: str,
    expected_data_columns: frozenset[str],
    numeric_sql_types: dict[str, str],
    key_columns: tuple[str, ...],
    unique_key_columns: tuple[str, ...] = (),
) -> None:
    bronze_df = spark.table(source_table)
    _validate_columns(bronze_df, expected_data_columns, source_table)
    cast_failures = _cast_failure_counts(bronze_df, numeric_sql_types)
    if cast_failures:
        raise ValueError(
            f"Invalid numeric values found in {source_table}: {cast_failures}"
        )

    silver_df = _build_silver_dataframe(bronze_df, numeric_sql_types)
    bronze_row_count = bronze_df.count()
    silver_row_count = silver_df.count()
    if silver_row_count != bronze_row_count:
        raise RuntimeError(
            f"Silver transformation changed row count for {source_table}: "
            f"bronze={bronze_row_count}, silver={silver_row_count}"
        )

    null_key_condition = F.lit(False)
    for column_name in key_columns:
        null_key_condition = null_key_condition | F.col(column_name).isNull()
    null_key_count = silver_df.where(null_key_condition).count()
    if null_key_count:
        raise ValueError(
            f"Null keys found in {source_table}: {null_key_count}"
        )

    duplicate_key_count = 0
    if unique_key_columns:
        duplicate_key_count = (
            silver_df.groupBy(*unique_key_columns)
            .count()
            .where(F.col("count") > 1)
            .count()
        )
        if duplicate_key_count:
            raise ValueError(
                f"Duplicate keys found in {source_table}: "
                f"{duplicate_key_count}"
            )

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )
    written_row_count = spark.table(target_table).count()
    if written_row_count != bronze_row_count:
        raise RuntimeError(
            f"Silver row-count mismatch for {target_table}: "
            f"bronze={bronze_row_count}, table={written_row_count}"
        )

    print(f"Silver table: {target_table}")
    print(f"Rows written and verified: {written_row_count:,}")
    print(
        f"Null keys: {null_key_count}; "
        f"duplicate keys: {duplicate_key_count}"
    )


def transform_previous_application(spark: DatabricksSession) -> None:
    _transform_related_table(
        spark,
        "credit_risk.bronze.previous_application",
        "credit_risk.silver.previous_application",
        PREVIOUS_APPLICATION_EXPECTED_DATA_COLUMNS,
        PREVIOUS_APPLICATION_NUMERIC_SQL_TYPES,
        ("SK_ID_PREV", "SK_ID_CURR"),
        ("SK_ID_PREV",),
    )


def transform_installments_payments(spark: DatabricksSession) -> None:
    _transform_related_table(
        spark,
        "credit_risk.bronze.installments_payments",
        "credit_risk.silver.installments_payments",
        INSTALLMENT_EXPECTED_DATA_COLUMNS,
        INSTALLMENT_NUMERIC_SQL_TYPES,
        ("SK_ID_PREV", "SK_ID_CURR", "NUM_INSTALMENT_NUMBER"),
    )


def main() -> None:
    spark = DatabricksSession.builder.serverless().getOrCreate()
    try:
        transform_application_train(spark)
        transform_bureau(spark)
        transform_previous_application(spark)
        transform_installments_payments(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
