from dataclasses import dataclass

import pandas as pd
import streamlit as st
from databricks.connect import DatabricksSession
from pyspark.sql import Column, DataFrame, functions as F


APPLICATION_TABLE = "credit_risk.silver.application_train"
BUREAU_TABLE = "credit_risk.silver.bureau"
OVERVIEW_TABLE = "credit_risk.gold.application_overview"
SEGMENT_TABLE = "credit_risk.gold.application_segment_metrics"
BUREAU_OVERVIEW_TABLE = "credit_risk.gold.bureau_overview"
BUREAU_STATUS_TABLE = "credit_risk.gold.bureau_status_metrics"
BUREAU_OVERDUE_TABLE = "credit_risk.gold.bureau_overdue_distribution"
BUREAU_DURATION_TABLE = "credit_risk.gold.bureau_duration_distribution"
BUREAU_EDUCATION_TABLE = "credit_risk.gold.bureau_education_metrics"
PREVIOUS_TABLE = "credit_risk.silver.previous_application"
PREVIOUS_OVERVIEW_TABLE = "credit_risk.gold.previous_application_overview"
PREVIOUS_STATUS_TABLE = "credit_risk.gold.previous_application_status_metrics"
PREVIOUS_PURPOSE_TABLE = "credit_risk.gold.previous_application_purpose_metrics"
INSTALLMENTS_TABLE = "credit_risk.silver.installments_payments"
INSTALLMENTS_OVERVIEW_TABLE = "credit_risk.gold.installments_overview"
INSTALLMENTS_STATUS_TABLE = "credit_risk.gold.installments_status_metrics"

GOLD_SEGMENT_DIMENSIONS = {
    "contract_type",
    "gender",
    "education",
    "income_type",
    "occupation",
    "family_status",
}
SEGMENT_COLUMNS = {
    "contract_type": "NAME_CONTRACT_TYPE",
    "gender": "CODE_GENDER",
    "education": "NAME_EDUCATION_TYPE",
    "income_type": "NAME_INCOME_TYPE",
    "occupation": "OCCUPATION_TYPE",
    "family_status": "NAME_FAMILY_STATUS",
}
SAMPLE_COLUMNS = {
    "overview": (
        "SK_ID_CURR",
        "TARGET",
        "NAME_CONTRACT_TYPE",
        "AMT_CREDIT",
        "AMT_INCOME_TOTAL",
        "AMT_ANNUITY",
    ),
    "demographics": (
        "SK_ID_CURR",
        "CODE_GENDER",
        "DAYS_BIRTH",
        "NAME_FAMILY_STATUS",
        "NAME_EDUCATION_TYPE",
        "CNT_CHILDREN",
        "TARGET",
    ),
    "loan": (
        "SK_ID_CURR",
        "NAME_CONTRACT_TYPE",
        "AMT_CREDIT",
        "AMT_INCOME_TOTAL",
        "AMT_ANNUITY",
        "AMT_GOODS_PRICE",
        "TARGET",
    ),
    "risk": (
        "SK_ID_CURR",
        "TARGET",
        "CODE_GENDER",
        "NAME_EDUCATION_TYPE",
        "NAME_CONTRACT_TYPE",
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
    ),
    "income": (
        "SK_ID_CURR",
        "AMT_INCOME_TOTAL",
        "NAME_INCOME_TYPE",
        "OCCUPATION_TYPE",
        "DAYS_EMPLOYED",
        "AMT_CREDIT",
        "TARGET",
    ),
}


@dataclass(frozen=True)
class DashboardFilters:
    contract_type: str = "All"
    gender: str = "All"
    income_min: int = 0
    income_max: int = 120_000_000
    credit_min: int = 0
    credit_max: int = 5_000_000
    target: int | None = None

    @property
    def is_default(self) -> bool:
        return self == DashboardFilters()


@st.cache_resource
def get_spark() -> DatabricksSession:
    return DatabricksSession.builder.serverless().getOrCreate()


def _filtered_applications(filters: DashboardFilters) -> DataFrame:
    applications = get_spark().table(APPLICATION_TABLE)
    if filters.contract_type != "All":
        applications = applications.where(
            F.col("NAME_CONTRACT_TYPE") == filters.contract_type
        )
    if filters.gender != "All":
        applications = applications.where(F.col("CODE_GENDER") == filters.gender)
    applications = applications.where(
        F.col("AMT_INCOME_TOTAL").between(
            filters.income_min, filters.income_max
        )
        & F.col("AMT_CREDIT").between(filters.credit_min, filters.credit_max)
    )
    if filters.target is not None:
        applications = applications.where(F.col("TARGET") == filters.target)
    return applications


def _target_1_count() -> Column:
    return F.coalesce(
        F.sum(F.when(F.col("TARGET") == 1, 1).otherwise(0)), F.lit(0)
    ).cast("long")


@st.cache_data(ttl="5m", max_entries=32)
def load_application_summary(
    filters: DashboardFilters,
) -> dict[str, object]:
    spark = get_spark()
    if filters.is_default:
        row = spark.table(OVERVIEW_TABLE).first()
        if row is None:
            raise ValueError(f"Gold overview table is empty: {OVERVIEW_TABLE}")
        return row.asDict(recursive=True)

    income = F.col("AMT_INCOME_TOTAL")
    applications = _filtered_applications(filters)
    row = applications.agg(
        F.count("*").alias("application_count"),
        _target_1_count().alias("target_1_count"),
        F.coalesce(
            F.sum(F.when(F.col("TARGET") == 0, 1).otherwise(0)), F.lit(0)
        )
        .cast("long")
        .alias("target_0_count"),
        F.avg("TARGET").alias("target_1_rate"),
        F.sum("AMT_CREDIT").alias("total_credit_amount"),
        F.avg("AMT_CREDIT").alias("average_credit_amount"),
        F.avg(F.when(F.col("TARGET") == 1, F.col("AMT_CREDIT"))).alias(
            "target_1_average_credit"
        ),
        F.avg(F.when(F.col("TARGET") == 1, F.col("AMT_INCOME_TOTAL"))).alias(
            "target_1_average_income"
        ),
        F.avg(F.when(F.col("TARGET") == 1, F.col("AMT_ANNUITY"))).alias(
            "target_1_average_annuity"
        ),
        F.avg(
            F.when(F.col("DAYS_EMPLOYED") < 0, F.col("TARGET"))
        ).alias("target_1_rate_recorded_tenure"),
        F.corr("AMT_INCOME_TOTAL", "AMT_CREDIT").alias(
            "income_credit_correlation"
        ),
        F.expr("percentile_approx(AMT_CREDIT, 0.5, 10000)").alias(
            "median_credit_amount"
        ),
        F.avg("AMT_INCOME_TOTAL").alias("average_income_total"),
        F.expr("percentile_approx(AMT_INCOME_TOTAL, 0.5, 10000)").alias(
            "median_income_total"
        ),
        F.avg("AMT_ANNUITY").alias("average_annuity_amount"),
        F.avg("AMT_GOODS_PRICE").alias("average_goods_price"),
        F.avg(
            F.when(income > 0, F.col("AMT_CREDIT") / income)
        ).alias("average_loan_to_income_ratio"),
        F.avg(
            F.when(income > 0, F.col("AMT_ANNUITY") / income)
        ).alias("average_annuity_to_income_ratio"),
        F.avg(-F.col("DAYS_BIRTH") / 365.25).alias("average_age_years"),
        F.avg("CNT_FAM_MEMBERS").alias("average_family_size"),
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
        F.avg(
            F.when(
                F.col("DAYS_EMPLOYED") < 0,
                -F.col("DAYS_EMPLOYED") / 365.25,
            )
        ).alias("average_employment_years"),
    ).first()
    if row is None:
        raise RuntimeError("The application summary query returned no result.")
    return row.asDict(recursive=True)


def _dimension_expression(dimension: str) -> Column:
    if dimension in SEGMENT_COLUMNS:
        return F.coalesce(
            F.col(SEGMENT_COLUMNS[dimension]).cast("string"), F.lit("Unknown")
        )
    if dimension == "age_band":
        age = -F.col("DAYS_BIRTH") / 365.25
        return (
            F.when(age < 25, "<25")
            .when(age < 30, "25-29")
            .when(age < 35, "30-34")
            .when(age < 40, "35-39")
            .when(age < 45, "40-44")
            .when(age < 50, "45-49")
            .when(age < 55, "50-54")
            .when(age < 60, "55-59")
            .otherwise("60+")
        )
    if dimension == "income_band":
        income = F.col("AMT_INCOME_TOTAL")
        return (
            F.when(income < 75_000, "<75k")
            .when(income < 150_000, "75k-150k")
            .when(income < 300_000, "150k-300k")
            .when(income < 600_000, "300k-600k")
            .otherwise("600k+")
        )
    if dimension == "employment_band":
        days_employed = F.col("DAYS_EMPLOYED")
        years = -days_employed / 365.25
        return (
            F.when(days_employed == 365243, "Unavailable")
            .when(years < 1, "0-1 years")
            .when(years < 3, "1-3 years")
            .when(years < 5, "3-5 years")
            .when(years < 10, "5-10 years")
            .when(years < 15, "10-15 years")
            .when(years < 20, "15-20 years")
            .otherwise("20+ years")
        )
    raise ValueError(f"Unsupported application segment: {dimension}")


def _aggregate_segments(
    applications: DataFrame,
    dimension: str,
) -> pd.DataFrame:
    segment_column = _dimension_expression(dimension)
    result = (
        applications.withColumn("segment_value", segment_column)
        .groupBy("segment_value")
        .agg(
            F.count("*").alias("application_count"),
            _target_1_count().alias("target_1_count"),
            F.avg("TARGET").alias("target_1_rate"),
            F.avg("AMT_CREDIT").alias("average_credit_amount"),
            F.avg("AMT_INCOME_TOTAL").alias("average_income_total"),
        )
        .orderBy(F.desc("target_1_rate"), F.asc("segment_value"))
        .toPandas()
    )
    return result.reset_index(drop=True)


SEGMENT_VALUE_ORDER = {
    "age_band": (
        "<25",
        "25-29",
        "30-34",
        "35-39",
        "40-44",
        "45-49",
        "50-54",
        "55-59",
        "60+",
    ),
    "income_band": ("<75k", "75k-150k", "150k-300k", "300k-600k", "600k+"),
    "employment_band": (
        "0-1 years",
        "1-3 years",
        "3-5 years",
        "5-10 years",
        "10-15 years",
        "15-20 years",
        "20+ years",
        "Unavailable",
    ),
}


def _order_segment_metrics(
    metrics: pd.DataFrame,
    dimension: str,
) -> pd.DataFrame:
    if metrics.empty:
        return metrics
    if dimension == "family_status":
        return metrics.sort_values(
            ["application_count", "segment_value"],
            ascending=[False, True],
            ignore_index=True,
        )
    category_order = SEGMENT_VALUE_ORDER.get(dimension)
    if category_order is None:
        return metrics.sort_values(
            ["target_1_rate", "segment_value"],
            ascending=[False, True],
            ignore_index=True,
        )
    order = {value: index for index, value in enumerate(category_order)}
    return (
        metrics.assign(
            _category_order=metrics["segment_value"].map(order).fillna(len(order))
        )
        .sort_values(["_category_order", "segment_value"], ignore_index=True)
        .drop(columns="_category_order")
    )


@st.cache_data(ttl="5m", max_entries=64)
def load_segment_metrics(
    filters: DashboardFilters,
    dimension: str,
) -> pd.DataFrame:
    if dimension in GOLD_SEGMENT_DIMENSIONS and filters.is_default:
        metrics = (
            get_spark()
            .table(SEGMENT_TABLE)
            .where(F.col("segment_type") == dimension)
            .drop("_built_at")
            .toPandas()
            .reset_index(drop=True)
        )
    else:
        metrics = _aggregate_segments(_filtered_applications(filters), dimension)
    return _order_segment_metrics(metrics, dimension)


def _histogram_parameters(
    metric: str,
) -> tuple[Column, float, float, str]:
    if metric == "credit_amount":
        return F.col("AMT_CREDIT"), 100_000.0, 3_000_000.0, "amount"
    if metric == "income_amount":
        return F.col("AMT_INCOME_TOTAL"), 50_000.0, 1_000_000.0, "amount"
    if metric == "loan_to_income":
        income = F.col("AMT_INCOME_TOTAL")
        return (
            F.when(income > 0, F.col("AMT_CREDIT") / income),
            0.5,
            10.0,
            "ratio",
        )
    if metric == "age_years":
        return -F.col("DAYS_BIRTH") / 365.25, 5.0, 80.0, "years"
    if metric == "employment_years":
        return -F.col("DAYS_EMPLOYED") / 365.25, 1.0, 30.0, "years"
    raise ValueError(f"Unsupported histogram metric: {metric}")


def _format_bucket(
    bucket_start: float,
    width: float,
    maximum: float,
    unit: str,
) -> str:
    if bucket_start >= maximum:
        if unit == "ratio":
            return f"{maximum:.1f}+"
        if unit == "years":
            return f"{maximum:.0f}+ years"
        return f"{maximum:,.0f}+"
    bucket_end = bucket_start + width
    if unit == "ratio":
        return f"{bucket_start:.1f}-{bucket_end:.1f}"
    if unit == "years":
        return f"{bucket_start:.0f}-{bucket_end:.0f} years"
    return f"{bucket_start:,.0f}-{bucket_end:,.0f}"


@st.cache_data(ttl="5m", max_entries=64)
def load_application_histogram(
    filters: DashboardFilters,
    metric: str,
) -> pd.DataFrame:
    value, width, maximum, unit = _histogram_parameters(metric)
    applications = _filtered_applications(filters).where(value.isNotNull())
    if metric == "employment_years":
        applications = applications.where(F.col("DAYS_EMPLOYED") < 0)
    bucket_start = F.when(
        value >= maximum, F.lit(maximum)
    ).otherwise(F.floor(value / width) * width)
    result = (
        applications.select(bucket_start.alias("bucket_start"))
        .groupBy("bucket_start")
        .agg(F.count("*").alias("application_count"))
        .orderBy("bucket_start")
        .toPandas()
    )
    if result.empty:
        return result
    result["bucket_label"] = result["bucket_start"].map(
        lambda value: _format_bucket(float(value), width, maximum, unit)
    )
    return result[["bucket_label", "application_count"]].reset_index(drop=True)


@st.cache_data(ttl="5m", max_entries=32)
def load_ratio_by_target(filters: DashboardFilters) -> pd.DataFrame:
    income = F.col("AMT_INCOME_TOTAL")
    applications = _filtered_applications(filters).where(income > 0)
    return (
        applications.withColumn(
            "target_group",
            F.when(
                F.col("TARGET") == 1,
                "TARGET=1 payment difficulties",
            ).otherwise("TARGET=0"),
        )
        .groupBy("target_group")
        .agg(
            F.count("*").alias("application_count"),
            F.avg(F.col("AMT_ANNUITY") / income).alias(
                "average_annuity_to_income_ratio"
            ),
        )
        .orderBy("target_group")
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_contract_credit_totals(filters: DashboardFilters) -> pd.DataFrame:
    return (
        _filtered_applications(filters)
        .groupBy("NAME_CONTRACT_TYPE")
        .agg(
            F.count("*").alias("application_count"),
            F.sum("AMT_CREDIT").alias("total_credit_amount"),
        )
        .orderBy(F.desc("total_credit_amount"))
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_occupation_income(filters: DashboardFilters) -> pd.DataFrame:
    return (
        _filtered_applications(filters)
        .withColumn(
            "occupation",
            F.coalesce(F.col("OCCUPATION_TYPE"), F.lit("Unknown")),
        )
        .groupBy("occupation")
        .agg(
            F.count("*").alias("application_count"),
            F.avg("AMT_INCOME_TOTAL").alias("average_income_total"),
            F.avg("TARGET").alias("target_1_rate"),
        )
        .orderBy(F.desc("average_income_total"))
        .limit(8)
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_income_credit_sample(filters: DashboardFilters) -> pd.DataFrame:
    return (
        _filtered_applications(filters)
        .where(
            F.col("AMT_INCOME_TOTAL").isNotNull()
            & F.col("AMT_CREDIT").isNotNull()
        )
        .sample(False, 0.02, 42)
        .select(
            "AMT_INCOME_TOTAL",
            "AMT_CREDIT",
            "TARGET",
            "NAME_CONTRACT_TYPE",
        )
        .limit(2000)
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_application_sample(
    filters: DashboardFilters,
    view: str,
    risk_only: bool = False,
) -> pd.DataFrame:
    if view not in SAMPLE_COLUMNS:
        raise ValueError(f"Unsupported application sample view: {view}")
    applications = _filtered_applications(filters)
    if risk_only:
        applications = applications.where(F.col("TARGET") == 1)
    return (
        applications.select(*SAMPLE_COLUMNS[view])
        .orderBy("SK_ID_CURR")
        .limit(100)
        .toPandas()
        .reset_index(drop=True)
    )


def _bureau_for_filters(filters: DashboardFilters) -> DataFrame:
    application_ids = _filtered_applications(filters).select(
        "SK_ID_CURR"
    ).distinct()
    return get_spark().table(BUREAU_TABLE).join(
        application_ids, "SK_ID_CURR", "inner"
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_bureau_summary(filters: DashboardFilters) -> dict[str, object]:
    if filters.is_default:
        row = get_spark().table(BUREAU_OVERVIEW_TABLE).first()
        if row is None:
            raise ValueError(
                f"Gold bureau overview table is empty: {BUREAU_OVERVIEW_TABLE}"
            )
        return row.asDict(recursive=True)
    bureau = _bureau_for_filters(filters)
    applications = _filtered_applications(filters).select("SK_ID_CURR", "TARGET")
    bureau_by_customer = bureau.groupBy("SK_ID_CURR").agg(
        F.sum("AMT_CREDIT_SUM").alias("customer_bureau_credit")
    )
    applicant_bureau = applications.join(
        bureau_by_customer, "SK_ID_CURR", "inner"
    )
    row = bureau.agg(
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
    ).crossJoin(
        applicant_bureau.agg(
            F.avg("customer_bureau_credit").alias(
                "average_customer_bureau_credit"
            ),
            F.avg("TARGET").alias("target_1_rate"),
        )
    ).first()
    if row is None:
        raise RuntimeError("The bureau summary query returned no result.")
    return row.asDict(recursive=True)


@st.cache_data(ttl="5m", max_entries=32)
def load_bureau_status_metrics(filters: DashboardFilters) -> pd.DataFrame:
    if filters.is_default:
        return (
            get_spark()
            .table(BUREAU_STATUS_TABLE)
            .drop("_built_at")
            .toPandas()
            .reset_index(drop=True)
        )
    return (
        _bureau_for_filters(filters)
        .withColumn(
            "credit_status",
            F.coalesce(F.col("CREDIT_ACTIVE"), F.lit("Unknown")),
        )
        .groupBy("credit_status")
        .agg(
            F.count("*").alias("credit_count"),
            F.sum("AMT_CREDIT_SUM").alias("total_credit_amount"),
            F.sum("AMT_CREDIT_SUM_OVERDUE").alias("total_overdue_amount"),
        )
        .orderBy(F.desc("credit_count"))
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_bureau_overdue_distribution(
    filters: DashboardFilters,
) -> pd.DataFrame:
    if filters.is_default:
        return (
            get_spark()
            .table(BUREAU_OVERDUE_TABLE)
            .drop("_built_at")
            .toPandas()
            .reset_index(drop=True)
        )
    overdue = F.coalesce(F.col("AMT_CREDIT_SUM_OVERDUE"), F.lit(0.0))
    bucket = (
        F.when(overdue == 0, "0")
        .when(overdue < 1_000, "1-999")
        .when(overdue < 5_000, "1,000-4,999")
        .when(overdue < 10_000, "5,000-9,999")
        .when(overdue < 50_000, "10,000-49,999")
        .otherwise("50,000+")
    )
    result = (
        _bureau_for_filters(filters)
        .withColumn("overdue_bucket", bucket)
        .groupBy("overdue_bucket")
        .agg(F.count("*").alias("credit_count"))
        .toPandas()
    )
    order = {
        label: index
        for index, label in enumerate(
            ("0", "1-999", "1,000-4,999", "5,000-9,999", "10,000-49,999", "50,000+")
        )
    }
    result["_order"] = result["overdue_bucket"].map(order)
    return (
        result.sort_values("_order")
        .drop(columns="_order")
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_bureau_duration_distribution(
    filters: DashboardFilters,
) -> pd.DataFrame:
    if filters.is_default:
        return (
            get_spark()
            .table(BUREAU_DURATION_TABLE)
            .drop("_built_at")
            .toPandas()
            .reset_index(drop=True)
        )
    credit_age = F.abs(F.col("DAYS_CREDIT"))
    bucket = (
        F.when(credit_age <= 180, "0-180")
        .when(credit_age <= 365, "181-365")
        .when(credit_age <= 730, "366-730")
        .when(credit_age <= 1_095, "731-1,095")
        .when(credit_age <= 1_825, "1,096-1,825")
        .when(credit_age <= 2_555, "1,826-2,555")
        .otherwise("2,556+")
    )
    result = (
        _bureau_for_filters(filters)
        .withColumn("credit_age_bucket", bucket)
        .groupBy("credit_age_bucket")
        .agg(F.count("*").alias("credit_count"))
        .toPandas()
    )
    order = {
        label: index
        for index, label in enumerate(
            (
                "0-180",
                "181-365",
                "366-730",
                "731-1,095",
                "1,096-1,825",
                "1,826-2,555",
                "2,556+",
            )
        )
    }
    result["_order"] = result["credit_age_bucket"].map(order)
    return (
        result.sort_values("_order")
        .drop(columns="_order")
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_bureau_education_metrics(
    filters: DashboardFilters,
) -> pd.DataFrame:
    if filters.is_default:
        return (
            get_spark()
            .table(BUREAU_EDUCATION_TABLE)
            .drop("_built_at")
            .toPandas()
            .reset_index(drop=True)
        )
    applications = _filtered_applications(filters).select(
        "SK_ID_CURR",
        "TARGET",
        "NAME_EDUCATION_TYPE",
    )
    bureau_by_customer = (
        _bureau_for_filters(filters)
        .groupBy("SK_ID_CURR")
        .agg(
            F.count("*").alias("bureau_record_count"),
            F.sum("AMT_CREDIT_SUM").alias("customer_bureau_credit"),
            F.sum("AMT_CREDIT_SUM_OVERDUE").alias(
                "customer_bureau_overdue"
            ),
        )
    )
    return (
        applications.join(bureau_by_customer, "SK_ID_CURR", "inner")
        .withColumn(
            "education",
            F.coalesce(F.col("NAME_EDUCATION_TYPE"), F.lit("Unknown")),
        )
        .groupBy("education")
        .agg(
            F.count("*").alias("customer_count"),
            F.avg("customer_bureau_credit").alias(
                "average_bureau_credit"
            ),
            F.avg("TARGET").alias("target_1_rate"),
        )
        .orderBy(F.desc("average_bureau_credit"))
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_bureau_sample(filters: DashboardFilters) -> pd.DataFrame:
    applications = _filtered_applications(filters).select(
        "SK_ID_CURR"
    ).distinct()
    return (
        get_spark()
        .table(BUREAU_TABLE)
        .join(applications, "SK_ID_CURR", "inner")
        .select(
            "SK_ID_CURR",
            "SK_ID_BUREAU",
            "CREDIT_ACTIVE",
            "CREDIT_TYPE",
            "DAYS_CREDIT",
            "AMT_CREDIT_SUM",
            "AMT_CREDIT_SUM_DEBT",
            "AMT_CREDIT_SUM_OVERDUE",
        )
        .orderBy("SK_ID_CURR", "SK_ID_BUREAU")
        .limit(100)
        .toPandas()
        .reset_index(drop=True)
    )


def _previous_for_filters(filters: DashboardFilters) -> DataFrame:
    application_ids = _filtered_applications(filters).select(
        "SK_ID_CURR"
    ).distinct()
    return get_spark().table(PREVIOUS_TABLE).join(
        application_ids, "SK_ID_CURR", "inner"
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_previous_application_summary(
    filters: DashboardFilters,
) -> dict[str, object]:
    if filters.is_default:
        row = get_spark().table(PREVIOUS_OVERVIEW_TABLE).first()
        if row is None:
            raise ValueError(
                f"Gold previous-application overview is empty: "
                f"{PREVIOUS_OVERVIEW_TABLE}"
            )
        return row.asDict(recursive=True)

    previous = _previous_for_filters(filters)
    row = previous.agg(
        F.count("*").alias("previous_application_count"),
        F.countDistinct("SK_ID_CURR").alias("customer_count"),
        F.sum(
            F.when(F.col("NAME_CONTRACT_STATUS") == "Approved", 1).otherwise(0)
        )
        .cast("long")
        .alias("approved_count"),
        F.sum(
            F.when(F.col("NAME_CONTRACT_STATUS") == "Refused", 1).otherwise(0)
        )
        .cast("long")
        .alias("refused_count"),
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
    ).first()
    if row is None:
        raise RuntimeError("The previous-application query returned no result.")
    result = row.asDict(recursive=True)
    approved = result.get("approved_count") or 0
    refused = result.get("refused_count") or 0
    result["approval_rate"] = (
        approved / (approved + refused) if approved + refused else None
    )
    return result


@st.cache_data(ttl="5m", max_entries=32)
def load_previous_status_metrics(
    filters: DashboardFilters,
) -> pd.DataFrame:
    if filters.is_default:
        return (
            get_spark()
            .table(PREVIOUS_STATUS_TABLE)
            .drop("_built_at")
            .toPandas()
            .reset_index(drop=True)
        )
    applications = _filtered_applications(filters).select(
        "SK_ID_CURR", "TARGET"
    )
    return (
        _previous_for_filters(filters)
        .join(applications, "SK_ID_CURR", "inner")
        .withColumn(
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
        .orderBy(F.desc("application_count"))
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_previous_purpose_metrics(
    filters: DashboardFilters,
) -> pd.DataFrame:
    if filters.is_default:
        return (
            get_spark()
            .table(PREVIOUS_PURPOSE_TABLE)
            .drop("_built_at")
            .orderBy(F.desc("application_count"))
            .limit(10)
            .toPandas()
            .reset_index(drop=True)
        )
    return (
        _previous_for_filters(filters)
        .withColumn(
            "loan_purpose",
            F.coalesce(F.col("NAME_CASH_LOAN_PURPOSE"), F.lit("Unknown")),
        )
        .groupBy("loan_purpose")
        .agg(
            F.count("*").alias("application_count"),
            F.avg("AMT_APPLICATION").alias("average_requested_amount"),
        )
        .orderBy(F.desc("application_count"))
        .limit(10)
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_previous_application_sample(
    filters: DashboardFilters,
) -> pd.DataFrame:
    return (
        _previous_for_filters(filters)
        .select(
            "SK_ID_CURR",
            "SK_ID_PREV",
            "NAME_CONTRACT_TYPE",
            "NAME_CONTRACT_STATUS",
            "AMT_APPLICATION",
            "AMT_CREDIT",
            "DAYS_DECISION",
            "CODE_REJECT_REASON",
        )
        .orderBy("SK_ID_CURR", "SK_ID_PREV")
        .limit(100)
        .toPandas()
        .reset_index(drop=True)
    )


def _installments_for_filters(filters: DashboardFilters) -> DataFrame:
    application_ids = _filtered_applications(filters).select(
        "SK_ID_CURR"
    ).distinct()
    return get_spark().table(INSTALLMENTS_TABLE).join(
        application_ids, "SK_ID_CURR", "inner"
    )


def _installment_days_late() -> Column:
    return F.col("DAYS_ENTRY_PAYMENT") - F.col("DAYS_INSTALMENT")


def _installment_shortfall() -> Column:
    return F.greatest(
        F.col("AMT_INSTALMENT") - F.coalesce(F.col("AMT_PAYMENT"), F.lit(0.0)),
        F.lit(0.0),
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_installments_summary(
    filters: DashboardFilters,
) -> dict[str, object]:
    if filters.is_default:
        row = get_spark().table(INSTALLMENTS_OVERVIEW_TABLE).first()
        if row is None:
            raise ValueError(
                f"Gold installments overview is empty: "
                f"{INSTALLMENTS_OVERVIEW_TABLE}"
            )
        return row.asDict(recursive=True)
    row = _installments_for_filters(filters).agg(
        F.count("*").alias("installment_count"),
        F.countDistinct("SK_ID_CURR").alias("customer_count"),
        F.sum("AMT_INSTALMENT").alias("scheduled_amount"),
        F.sum("AMT_PAYMENT").alias("paid_amount"),
        F.sum(_installment_shortfall()).alias("payment_shortfall"),
        F.sum(F.when(_installment_days_late() > 0, 1).otherwise(0))
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
        F.avg(
            F.when(_installment_days_late() > 0, _installment_days_late())
        ).alias("average_days_late"),
    ).first()
    if row is None:
        raise RuntimeError("The installments query returned no result.")
    return row.asDict(recursive=True)


@st.cache_data(ttl="5m", max_entries=32)
def load_installments_status_metrics(
    filters: DashboardFilters,
) -> pd.DataFrame:
    if filters.is_default:
        return (
            get_spark()
            .table(INSTALLMENTS_STATUS_TABLE)
            .drop("_built_at")
            .toPandas()
            .reset_index(drop=True)
        )
    payment_status = (
        F.when(F.col("DAYS_ENTRY_PAYMENT").isNull(), "No recorded payment")
        .when(_installment_days_late() > 0, "Paid late")
        .otherwise("Paid on time or early")
    )
    return (
        _installments_for_filters(filters)
        .withColumn("payment_status", payment_status)
        .groupBy("payment_status")
        .agg(
            F.count("*").alias("installment_count"),
            F.sum("AMT_INSTALMENT").alias("scheduled_amount"),
            F.sum("AMT_PAYMENT").alias("paid_amount"),
            F.sum(_installment_shortfall()).alias("payment_shortfall"),
            F.avg(
                F.when(_installment_days_late() > 0, _installment_days_late())
            ).alias("average_days_late"),
        )
        .orderBy(F.desc("installment_count"))
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=32)
def load_installments_sample(filters: DashboardFilters) -> pd.DataFrame:
    return (
        _installments_for_filters(filters)
        .withColumn("days_late", _installment_days_late())
        .withColumn("payment_shortfall", _installment_shortfall())
        .select(
            "SK_ID_CURR",
            "SK_ID_PREV",
            "NUM_INSTALMENT_NUMBER",
            "DAYS_INSTALMENT",
            "DAYS_ENTRY_PAYMENT",
            "days_late",
            "AMT_INSTALMENT",
            "AMT_PAYMENT",
            "payment_shortfall",
        )
        .orderBy("SK_ID_CURR", "SK_ID_PREV", "NUM_INSTALMENT_NUMBER")
        .limit(100)
        .toPandas()
        .reset_index(drop=True)
    )


@st.cache_data(ttl="5m", max_entries=16)
def load_prediction_training_sample(
    filters: DashboardFilters,
) -> pd.DataFrame:
    features = (
        "SK_ID_CURR",
        "TARGET",
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "AMT_ANNUITY",
        "AMT_GOODS_PRICE",
        "DAYS_BIRTH",
        "DAYS_EMPLOYED",
        "EXT_SOURCE_1",
        "EXT_SOURCE_2",
        "EXT_SOURCE_3",
        "CNT_CHILDREN",
    )
    return (
        _filtered_applications(filters)
        .sample(False, 0.12, seed=42)
        .select(*features)
        .limit(30_000)
        .toPandas()
        .reset_index(drop=True)
    )
