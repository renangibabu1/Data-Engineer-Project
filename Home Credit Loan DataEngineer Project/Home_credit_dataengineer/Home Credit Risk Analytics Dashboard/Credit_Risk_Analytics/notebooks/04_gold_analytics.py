from databricks.connect import DatabricksSession
from pyspark.sql import DataFrame, functions as F


SOURCE_TABLE = "credit_risk.silver.application_train"
OVERVIEW_TABLE = "credit_risk.gold.application_overview"
SEGMENT_TABLE = "credit_risk.gold.application_segment_metrics"
BUREAU_SOURCE_TABLE = "credit_risk.silver.bureau"
BUREAU_OVERVIEW_TABLE = "credit_risk.gold.bureau_overview"
BUREAU_STATUS_TABLE = "credit_risk.gold.bureau_status_metrics"
BUREAU_OVERDUE_TABLE = "credit_risk.gold.bureau_overdue_distribution"
BUREAU_DURATION_TABLE = "credit_risk.gold.bureau_duration_distribution"
BUREAU_EDUCATION_TABLE = "credit_risk.gold.bureau_education_metrics"
PREVIOUS_SOURCE_TABLE = "credit_risk.silver.previous_application"
PREVIOUS_OVERVIEW_TABLE = "credit_risk.gold.previous_application_overview"
PREVIOUS_STATUS_TABLE = "credit_risk.gold.previous_application_status_metrics"
PREVIOUS_PURPOSE_TABLE = "credit_risk.gold.previous_application_purpose_metrics"
INSTALLMENTS_SOURCE_TABLE = "credit_risk.silver.installments_payments"
INSTALLMENTS_OVERVIEW_TABLE = "credit_risk.gold.installments_overview"
INSTALLMENTS_STATUS_TABLE = "credit_risk.gold.installments_status_metrics"
SEGMENT_DIMENSIONS = (
    ("contract_type", "NAME_CONTRACT_TYPE"),
    ("gender", "CODE_GENDER"),
    ("education", "NAME_EDUCATION_TYPE"),
    ("income_type", "NAME_INCOME_TYPE"),
    ("occupation", "OCCUPATION_TYPE"),
    ("family_status", "NAME_FAMILY_STATUS"),
)


def build_application_overview(silver_df: DataFrame) -> DataFrame:
    income = F.col("AMT_INCOME_TOTAL")
    employment_years = -F.col("DAYS_EMPLOYED") / 365.25
    overview = silver_df.agg(
        F.count("*").alias("application_count"),
        F.sum(F.when(F.col("TARGET") == 1, 1).otherwise(0))
        .cast("long")
        .alias("target_1_count"),
        F.sum(F.when(F.col("TARGET") == 0, 1).otherwise(0))
        .cast("long")
        .alias("target_0_count"),
        F.round(F.avg("AMT_CREDIT"), 2).alias("average_credit_amount"),
        F.round(
            F.avg(F.when(F.col("TARGET") == 1, F.col("AMT_CREDIT"))), 2
        ).alias("target_1_average_credit"),
        F.round(
            F.avg(F.when(F.col("TARGET") == 1, F.col("AMT_INCOME_TOTAL"))), 2
        ).alias("target_1_average_income"),
        F.round(
            F.avg(F.when(F.col("TARGET") == 1, F.col("AMT_ANNUITY"))), 2
        ).alias("target_1_average_annuity"),
        F.round(
            F.avg(
                F.when(
                    F.col("DAYS_EMPLOYED") < 0,
                    F.col("TARGET"),
                )
            ),
            6,
        ).alias("target_1_rate_recorded_tenure"),
        F.round(
            F.corr("AMT_INCOME_TOTAL", "AMT_CREDIT"), 6
        ).alias("income_credit_correlation"),
        F.round(
            F.expr("percentile_approx(AMT_CREDIT, 0.5, 10000)"), 2
        ).alias("median_credit_amount"),
        F.round(F.avg("AMT_INCOME_TOTAL"), 2).alias("average_income_total"),
        F.round(
            F.expr("percentile_approx(AMT_INCOME_TOTAL, 0.5, 10000)"), 2
        ).alias("median_income_total"),
        F.round(F.avg("AMT_ANNUITY"), 2).alias("average_annuity_amount"),
        F.round(F.avg("AMT_GOODS_PRICE"), 2).alias("average_goods_price"),
        F.round(F.sum("AMT_CREDIT"), 2).alias("total_credit_amount"),
        F.round(
            F.avg(
                F.when(income > 0, F.col("AMT_CREDIT") / income)
            ),
            4,
        ).alias("average_loan_to_income_ratio"),
        F.round(
            F.avg(
                F.when(income > 0, F.col("AMT_ANNUITY") / income)
            ),
            4,
        ).alias("average_annuity_to_income_ratio"),
        F.round(F.avg(-F.col("DAYS_BIRTH") / 365.25), 2).alias(
            "average_age_years"
        ),
        F.round(F.avg("CNT_FAM_MEMBERS"), 2).alias("average_family_size"),
        F.sum(
            F.when(F.col("NAME_FAMILY_STATUS") == "Married", 1).otherwise(0)
        )
        .cast("long")
        .alias("married_application_count"),
        F.sum(F.when(F.col("CNT_CHILDREN") > 0, 1).otherwise(0))
        .cast("long")
        .alias("customers_with_children_count"),
        F.sum(
            F.when(F.col("CODE_GENDER") == "M", 1).otherwise(0)
        )
        .cast("long")
        .alias("male_application_count"),
        F.sum(
            F.when(F.col("CODE_GENDER") == "F", 1).otherwise(0)
        )
        .cast("long")
        .alias("female_application_count"),
        F.sum(
            F.when(F.col("NAME_CONTRACT_TYPE") == "Cash loans", 1).otherwise(0)
        )
        .cast("long")
        .alias("cash_loan_count"),
        F.sum(F.when(F.col("DAYS_EMPLOYED") < 0, 1).otherwise(0))
        .cast("long")
        .alias("employment_tenure_recorded_count"),
        F.sum(F.when(F.col("DAYS_EMPLOYED") == 365243, 1).otherwise(0))
        .cast("long")
        .alias("employment_tenure_unavailable_count"),
        F.round(
            F.avg(F.when(F.col("DAYS_EMPLOYED") < 0, employment_years)), 2
        ).alias("average_employment_years"),
    )
    return (
        overview.withColumn(
            "target_1_rate",
            F.round(F.col("target_1_count") / F.col("application_count"), 6),
        )
        .withColumn("_built_at", F.current_timestamp())
        .select(
            "application_count",
            "target_1_count",
            "target_0_count",
            "target_1_rate",
            "average_credit_amount",
            "target_1_average_credit",
            "target_1_average_income",
            "target_1_average_annuity",
            "target_1_rate_recorded_tenure",
            "income_credit_correlation",
            "median_credit_amount",
            "average_income_total",
            "median_income_total",
            "average_annuity_amount",
            "average_goods_price",
            "total_credit_amount",
            "average_loan_to_income_ratio",
            "average_annuity_to_income_ratio",
            "average_age_years",
            "average_family_size",
            "married_application_count",
            "customers_with_children_count",
            "male_application_count",
            "female_application_count",
            "cash_loan_count",
            "employment_tenure_recorded_count",
            "employment_tenure_unavailable_count",
            "average_employment_years",
            "_built_at",
        )
    )


def build_segment_metrics(silver_df: DataFrame) -> DataFrame:
    segment_frames = []
    for segment_type, column_name in SEGMENT_DIMENSIONS:
        segment_frame = (
            silver_df.groupBy(
                F.coalesce(F.col(column_name), F.lit("Unknown")).alias(
                    "segment_value"
                )
            )
            .agg(
                F.count("*").alias("application_count"),
                F.sum(F.when(F.col("TARGET") == 1, 1).otherwise(0))
                .cast("long")
                .alias("target_1_count"),
                F.round(F.avg("AMT_CREDIT"), 2).alias("average_credit_amount"),
                F.round(F.avg("AMT_INCOME_TOTAL"), 2).alias(
                    "average_income_total"
                ),
            )
            .withColumn("segment_type", F.lit(segment_type))
            .withColumn(
                "target_1_rate",
                F.round(
                    F.col("target_1_count") / F.col("application_count"),
                    6,
                ),
            )
            .withColumn("_built_at", F.current_timestamp())
            .select(
                "segment_type",
                "segment_value",
                "application_count",
                "target_1_count",
                "target_1_rate",
                "average_credit_amount",
                "average_income_total",
                "_built_at",
            )
        )
        segment_frames.append(segment_frame)

    combined = segment_frames[0]
    for segment_frame in segment_frames[1:]:
        combined = combined.unionByName(segment_frame)
    return combined


def build_bureau_analytics(
    applications: DataFrame,
    bureau: DataFrame,
) -> dict[str, DataFrame]:
    application_keys = applications.select("SK_ID_CURR")
    in_population = bureau.join(application_keys, "SK_ID_CURR", "inner")
    bureau_by_customer = in_population.groupBy("SK_ID_CURR").agg(
        F.count("*").alias("bureau_record_count"),
        F.sum("AMT_CREDIT_SUM").alias("customer_bureau_credit"),
        F.sum("AMT_CREDIT_SUM_OVERDUE").alias("customer_bureau_overdue"),
    )
    applicant_bureau = applications.select("SK_ID_CURR", "TARGET").join(
        bureau_by_customer, "SK_ID_CURR", "inner"
    )

    overview = (
        in_population.agg(
            F.count("*").alias("record_count"),
            F.countDistinct("SK_ID_CURR").alias("customer_count"),
            F.sum(F.when(F.col("CREDIT_ACTIVE") == "Active", 1).otherwise(0))
            .cast("long")
            .alias("active_credit_count"),
            F.sum(F.when(F.col("CREDIT_ACTIVE") == "Closed", 1).otherwise(0))
            .cast("long")
            .alias("closed_credit_count"),
            F.sum("AMT_CREDIT_SUM").alias("total_credit_amount"),
            F.sum("AMT_CREDIT_SUM_OVERDUE").alias("total_overdue_amount"),
            F.avg("AMT_CREDIT_SUM").alias("average_credit_amount"),
            F.avg(F.abs(F.col("DAYS_CREDIT"))).alias(
                "average_credit_age_days"
            ),
        )
        .crossJoin(
            applicant_bureau.agg(
                F.avg("customer_bureau_credit").alias(
                    "average_customer_bureau_credit"
                ),
                F.avg("TARGET").alias("target_1_rate"),
            )
        )
        .withColumn("_built_at", F.current_timestamp())
    )

    status_metrics = (
        in_population.withColumn(
            "credit_status",
            F.coalesce(F.col("CREDIT_ACTIVE"), F.lit("Unknown")),
        )
        .groupBy("credit_status")
        .agg(
            F.count("*").alias("credit_count"),
            F.sum("AMT_CREDIT_SUM").alias("total_credit_amount"),
            F.sum("AMT_CREDIT_SUM_OVERDUE").alias("total_overdue_amount"),
        )
        .withColumn("_built_at", F.current_timestamp())
        .select(
            "credit_status",
            "credit_count",
            "total_credit_amount",
            "total_overdue_amount",
            "_built_at",
        )
    )

    overdue = F.coalesce(F.col("AMT_CREDIT_SUM_OVERDUE"), F.lit(0.0))
    overdue_bucket = (
        F.when(overdue == 0, "0")
        .when(overdue < 1_000, "1-999")
        .when(overdue < 5_000, "1,000-4,999")
        .when(overdue < 10_000, "5,000-9,999")
        .when(overdue < 50_000, "10,000-49,999")
        .otherwise("50,000+")
    )
    overdue_order = (
        F.when(overdue == 0, 0)
        .when(overdue < 1_000, 1)
        .when(overdue < 5_000, 2)
        .when(overdue < 10_000, 3)
        .when(overdue < 50_000, 4)
        .otherwise(5)
    )
    overdue_metrics = (
        in_population.withColumn("overdue_bucket", overdue_bucket)
        .withColumn("bucket_order", overdue_order)
        .groupBy("overdue_bucket", "bucket_order")
        .agg(F.count("*").alias("credit_count"))
        .orderBy("bucket_order")
        .withColumn("_built_at", F.current_timestamp())
        .select("overdue_bucket", "credit_count", "_built_at")
    )

    credit_age = F.abs(F.col("DAYS_CREDIT"))
    age_bucket = (
        F.when(credit_age <= 180, "0-180")
        .when(credit_age <= 365, "181-365")
        .when(credit_age <= 730, "366-730")
        .when(credit_age <= 1_095, "731-1,095")
        .when(credit_age <= 1_825, "1,096-1,825")
        .when(credit_age <= 2_555, "1,826-2,555")
        .otherwise("2,556+")
    )
    age_order = (
        F.when(credit_age <= 180, 0)
        .when(credit_age <= 365, 1)
        .when(credit_age <= 730, 2)
        .when(credit_age <= 1_095, 3)
        .when(credit_age <= 1_825, 4)
        .when(credit_age <= 2_555, 5)
        .otherwise(6)
    )
    duration_metrics = (
        in_population.withColumn("credit_age_bucket", age_bucket)
        .withColumn("bucket_order", age_order)
        .groupBy("credit_age_bucket", "bucket_order")
        .agg(F.count("*").alias("credit_count"))
        .orderBy("bucket_order")
        .withColumn("_built_at", F.current_timestamp())
        .select("credit_age_bucket", "credit_count", "_built_at")
    )

    education_metrics = (
        applications.select(
            "SK_ID_CURR",
            "TARGET",
            "NAME_EDUCATION_TYPE",
        )
        .join(bureau_by_customer, "SK_ID_CURR", "inner")
        .withColumn(
            "education",
            F.coalesce(F.col("NAME_EDUCATION_TYPE"), F.lit("Unknown")),
        )
        .groupBy("education")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("customer_bureau_credit").alias("average_bureau_credit"),
            F.avg("TARGET").alias("target_1_rate"),
        )
        .withColumn("_built_at", F.current_timestamp())
        .select(
            "education",
            "customer_count",
            "average_bureau_credit",
            "target_1_rate",
            "_built_at",
        )
    )

    return {
        BUREAU_OVERVIEW_TABLE: overview,
        BUREAU_STATUS_TABLE: status_metrics,
        BUREAU_OVERDUE_TABLE: overdue_metrics,
        BUREAU_DURATION_TABLE: duration_metrics,
        BUREAU_EDUCATION_TABLE: education_metrics,
    }


def build_previous_application_analytics(
    applications: DataFrame,
    previous_applications: DataFrame,
) -> dict[str, DataFrame]:
    application_cohort = applications.select("SK_ID_CURR", "TARGET")
    previous = previous_applications.join(
        application_cohort, "SK_ID_CURR", "inner"
    )
    approved_count = F.sum(
        F.when(F.col("NAME_CONTRACT_STATUS") == "Approved", 1).otherwise(0)
    )
    refused_count = F.sum(
        F.when(F.col("NAME_CONTRACT_STATUS") == "Refused", 1).otherwise(0)
    )
    overview = (
        previous.agg(
            F.count("*").alias("previous_application_count"),
            F.countDistinct("SK_ID_CURR").alias("customer_count"),
            approved_count.cast("long").alias("approved_count"),
            refused_count.cast("long").alias("refused_count"),
            F.sum(
                F.when(F.col("NAME_CONTRACT_STATUS") == "Canceled", 1).otherwise(0)
            )
            .cast("long")
            .alias("canceled_count"),
            F.avg("AMT_APPLICATION").alias("average_requested_amount"),
            F.avg(
                F.when(
                    F.col("NAME_CONTRACT_STATUS") == "Approved",
                    F.col("AMT_CREDIT"),
                )
            ).alias("average_approved_credit"),
            F.avg("TARGET").alias("target_1_rate"),
        )
        .withColumn(
            "approval_rate",
            F.when(
                F.col("approved_count") + F.col("refused_count") > 0,
                F.col("approved_count")
                / (F.col("approved_count") + F.col("refused_count")),
            ),
        )
        .withColumn("_built_at", F.current_timestamp())
    )

    status_metrics = (
        previous.withColumn(
            "application_status",
            F.coalesce(F.col("NAME_CONTRACT_STATUS"), F.lit("Unknown")),
        )
        .groupBy("application_status")
        .agg(
            F.count("*").alias("application_count"),
            F.avg("AMT_APPLICATION").alias("average_requested_amount"),
            F.avg("AMT_CREDIT").alias("average_credit_amount"),
            F.avg("TARGET").alias("target_1_rate"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )
    purpose_metrics = (
        previous.withColumn(
            "loan_purpose",
            F.coalesce(F.col("NAME_CASH_LOAN_PURPOSE"), F.lit("Unknown")),
        )
        .groupBy("loan_purpose")
        .agg(
            F.count("*").alias("application_count"),
            F.avg("AMT_APPLICATION").alias("average_requested_amount"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )
    return {
        PREVIOUS_OVERVIEW_TABLE: overview,
        PREVIOUS_STATUS_TABLE: status_metrics,
        PREVIOUS_PURPOSE_TABLE: purpose_metrics,
    }


def build_installments_analytics(
    applications: DataFrame,
    installments: DataFrame,
) -> dict[str, DataFrame]:
    application_keys = applications.select("SK_ID_CURR").distinct()
    cohort_installments = installments.join(
        application_keys, "SK_ID_CURR", "inner"
    )
    days_late = F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT")
    shortfall = F.greatest(
        F.col("AMT_INSTALMENT") - F.coalesce(F.col("AMT_PAYMENT"), F.lit(0.0)),
        F.lit(0.0),
    )
    overview = (
        cohort_installments.agg(
            F.count("*").alias("installment_count"),
            F.countDistinct("SK_ID_CURR").alias("customer_count"),
            F.sum("AMT_INSTALMENT").alias("scheduled_amount"),
            F.sum("AMT_PAYMENT").alias("paid_amount"),
            F.sum(shortfall).alias("payment_shortfall"),
            F.sum(F.when(days_late > 0, 1).otherwise(0))
            .cast("long")
            .alias("late_installment_count"),
            F.sum(
                F.when(
                    F.col("AMT_PAYMENT").isNull()
                    | (F.col("AMT_PAYMENT") < F.col("AMT_INSTALMENT")),
                    1,
                ).otherwise(0)
            )
            .cast("long")
            .alias("unpaid_or_partial_installment_count"),
            F.avg(F.when(days_late > 0, days_late)).alias("average_days_late"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )

    payment_status = (
        F.when(F.col("DAYS_ENTRY_PAYMENT").isNull(), "No recorded payment")
        .when(days_late > 0, "Paid late")
        .otherwise("Paid on time or early")
    )
    status_metrics = (
        cohort_installments.withColumn("payment_status", payment_status)
        .groupBy("payment_status")
        .agg(
            F.count("*").alias("installment_count"),
            F.sum("AMT_INSTALMENT").alias("scheduled_amount"),
            F.sum("AMT_PAYMENT").alias("paid_amount"),
            F.sum(shortfall).alias("payment_shortfall"),
            F.avg(F.when(days_late > 0, days_late)).alias("average_days_late"),
        )
        .withColumn("_built_at", F.current_timestamp())
    )
    return {
        INSTALLMENTS_OVERVIEW_TABLE: overview,
        INSTALLMENTS_STATUS_TABLE: status_metrics,
    }


def build_gold_analytics(spark: DatabricksSession) -> None:
    silver_df = spark.table(SOURCE_TABLE)
    application_count = silver_df.count()
    if application_count == 0:
        raise ValueError(f"No data rows found in {SOURCE_TABLE}")

    overview_df = build_application_overview(silver_df)
    overview = overview_df.first()
    if overview["application_count"] != application_count:
        raise RuntimeError(
            "Gold overview application count does not match Silver"
        )
    if (
        overview["target_1_count"] + overview["target_0_count"]
        != application_count
    ):
        raise ValueError("TARGET must be 0 or 1 for every Silver application")

    segment_df = build_segment_metrics(silver_df)
    bureau_tables = build_bureau_analytics(
        silver_df, spark.table(BUREAU_SOURCE_TABLE)
    )
    previous_tables = build_previous_application_analytics(
        silver_df, spark.table(PREVIOUS_SOURCE_TABLE)
    )
    installment_tables = build_installments_analytics(
        silver_df, spark.table(INSTALLMENTS_SOURCE_TABLE)
    )
    related_tables = {
        **bureau_tables,
        **previous_tables,
        **installment_tables,
    }
    segment_totals = {
        row["segment_type"]: row["application_count"]
        for row in (
            segment_df.groupBy("segment_type")
            .agg(F.sum("application_count").alias("application_count"))
            .collect()
        )
    }
    expected_totals = {
        segment_type: application_count
        for segment_type, _ in SEGMENT_DIMENSIONS
    }
    if segment_totals != expected_totals:
        raise RuntimeError(
            "Gold segment totals do not reconcile to Silver: "
            f"expected={expected_totals}, actual={segment_totals}"
        )

    (
        overview_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(OVERVIEW_TABLE)
    )
    (
        segment_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(SEGMENT_TABLE)
    )
    for table_name, dataframe in related_tables.items():
        (
            dataframe.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )

    overview_row_count = spark.table(OVERVIEW_TABLE).count()
    segment_row_count = spark.table(SEGMENT_TABLE).count()
    if overview_row_count != 1:
        raise RuntimeError(
            f"Expected one Gold overview row, found {overview_row_count}"
        )
    if segment_row_count == 0:
        raise RuntimeError("Gold segment metrics table is empty")
    for table_name in related_tables:
        row_count = spark.table(table_name).count()
        if row_count == 0:
            raise RuntimeError(f"Gold analytics table is empty: {table_name}")
        if table_name in {
            BUREAU_OVERVIEW_TABLE,
            PREVIOUS_OVERVIEW_TABLE,
            INSTALLMENTS_OVERVIEW_TABLE,
        } and row_count != 1:
            raise RuntimeError(
                f"Expected one Gold overview row for {table_name}, "
                f"found {row_count}"
            )

    previous_application_count = spark.table(PREVIOUS_OVERVIEW_TABLE).first()[
        "previous_application_count"
    ]
    for table_name in (PREVIOUS_STATUS_TABLE, PREVIOUS_PURPOSE_TABLE):
        grouped_count = (
            spark.table(table_name)
            .agg(F.sum("application_count").alias("grouped_count"))
            .first()["grouped_count"]
        )
        if grouped_count != previous_application_count:
            raise RuntimeError(
                f"Previous-application metrics do not reconcile for "
                f"{table_name}: expected={previous_application_count}, "
                f"actual={grouped_count}"
            )

    installment_count = spark.table(INSTALLMENTS_OVERVIEW_TABLE).first()[
        "installment_count"
    ]
    grouped_installment_count = (
        spark.table(INSTALLMENTS_STATUS_TABLE)
        .agg(F.sum("installment_count").alias("grouped_count"))
        .first()["grouped_count"]
    )
    if grouped_installment_count != installment_count:
        raise RuntimeError(
            "Installment metrics do not reconcile: "
            f"expected={installment_count}, "
            f"actual={grouped_installment_count}"
        )

    print(f"Overview table: {OVERVIEW_TABLE} ({overview_row_count} row)")
    print(
        f"Segment table: {SEGMENT_TABLE} "
        f"({segment_row_count} rows across {len(SEGMENT_DIMENSIONS)} dimensions)"
    )
    print("Related analytics written: " + ", ".join(related_tables.keys()))
    print(f"Applications reconciled: {application_count:,}")
    print(
        "TARGET=1 is the source-defined payment-difficulty outcome; "
        "it is not relabeled as default."
    )


def main() -> None:
    spark = DatabricksSession.builder.serverless().getOrCreate()
    try:
        build_gold_analytics(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
