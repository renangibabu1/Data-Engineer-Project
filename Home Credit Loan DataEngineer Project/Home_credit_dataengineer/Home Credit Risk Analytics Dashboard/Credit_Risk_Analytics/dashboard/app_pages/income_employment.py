import streamlit as st

from common import (
    current_filters,
    format_amount,
    format_count,
    format_percent,
    render_bar_chart,
    render_data_table,
    render_metrics,
    render_scatter_chart,
)
from data_access import (
    load_application_histogram,
    load_application_sample,
    load_application_summary,
    load_income_credit_sample,
    load_occupation_income,
    load_segment_metrics,
)


filters = current_filters()
summary = load_application_summary(filters)
application_count = summary.get("application_count") or 0
tenure_recorded = summary.get("employment_tenure_recorded_count") or 0
tenure_share = tenure_recorded / application_count if application_count else None

st.caption(
    "Income and employment tenure summaries. The source uses 365243 in "
    "DAYS_EMPLOYED as a special unavailable value; it is not counted as years."
)
render_metrics(
    [
        ("Average income", format_amount(summary.get("average_income_total")), None),
        ("Median income", format_amount(summary.get("median_income_total")), None),
        (
            "Average recorded employment tenure",
            f"{format_amount(summary.get('average_employment_years'), 2)} years",
            None,
        ),
        ("Tenure recorded", format_percent(tenure_share), None),
        (
            "Tenure unavailable",
            format_count(summary.get("employment_tenure_unavailable_count")),
            None,
        ),
        (
            "TARGET=1 rate, recorded tenure",
            format_percent(summary.get("target_1_rate_recorded_tenure")),
            "Only applicants with recorded employment tenure are included.",
        ),
    ]
)

income_distribution = load_application_histogram(filters, "income_amount")
employment_distribution = load_application_histogram(filters, "employment_years")
occupation_income = load_occupation_income(filters).rename(
    columns={"occupation": "Occupation"}
)
occupation_risk = (
    load_segment_metrics(filters, "occupation")
    .rename(columns={"segment_value": "Occupation"})
    .head(8)
)
scatter = load_income_credit_sample(filters)
if not scatter.empty:
    scatter["outcome"] = scatter["TARGET"].map(
        {0: "TARGET=0", 1: "TARGET=1 payment difficulties"}
    )

row_one = st.columns(3)
with row_one[0]:
    render_bar_chart(
        "Income distribution",
        income_distribution,
        "bucket_label",
        "application_count",
        "Application counts by total income range.",
    )
with row_one[1]:
    render_bar_chart(
        "Average income by occupation (top 8)",
        occupation_income,
        "Occupation",
        "average_income_total",
        "Average income for the eight occupations with the highest average income.",
        horizontal=True,
    )
with row_one[2]:
    render_bar_chart(
        "Recorded employment-tenure distribution",
        employment_distribution,
        "bucket_label",
        "application_count",
        "Applications by recorded employment-tenure range; unavailable values are excluded.",
    )

row_two = st.columns(2)
with row_two[0]:
    render_bar_chart(
        "Highest TARGET=1 rates by occupation (top 8)",
        occupation_risk,
        "Occupation",
        "target_1_rate",
        "Payment-difficulty rate for the eight occupation groups with the highest observed rates.",
        horizontal=True,
    )
with row_two[1]:
    render_scatter_chart(
        "Income vs. credit amount (sample)",
        scatter,
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "Sample of applicant income and requested credit amounts, colored by TARGET.",
        note=(
            "Population Pearson correlation: "
            f"{format_amount(summary.get('income_credit_correlation'), 3)} "
            "(computed across the filtered cohort)."
        ),
    )

render_data_table(
    "Income and employment details (sample)",
    load_application_sample(filters, "income"),
    "Sample income and employment records with application TARGET values.",
    "income_employment_sample.csv",
)
