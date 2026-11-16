import pandas as pd
import streamlit as st

from common import (
    current_filters,
    format_amount,
    format_count,
    format_percent,
    render_bar_chart,
    render_data_table,
    render_donut_chart,
    render_metrics,
    render_scatter_chart,
)
from data_access import (
    load_application_histogram,
    load_application_sample,
    load_application_summary,
    load_income_credit_sample,
    load_segment_metrics,
)


filters = current_filters()
summary = load_application_summary(filters)
application_count = summary.get("application_count")
target_1_rate = summary.get("target_1_rate")

st.caption(
    "Portfolio summary for the application training cohort. "
    "TARGET=1 is the source-defined payment-difficulty outcome."
)
render_metrics(
    [
        ("Applications", format_count(application_count), None),
        (
            "TARGET=1 applications",
            format_count(summary.get("target_1_count")),
            f"{format_percent(target_1_rate)} of this filtered cohort.",
        ),
        ("TARGET=1 rate", format_percent(target_1_rate), None),
        ("TARGET=0 applications", format_count(summary.get("target_0_count")), None),
        (
            "Average credit amount",
            format_amount(summary.get("average_credit_amount")),
            None,
        ),
        (
            "Average income",
            format_amount(summary.get("average_income_total")),
            None,
        ),
    ]
)

target_data = pd.DataFrame(
    [
        {
            "Outcome": "TARGET=0",
            "Applications": summary.get("target_0_count", 0),
        },
        {
            "Outcome": "TARGET=1 payment difficulties",
            "Applications": summary.get("target_1_count", 0),
        },
    ]
)
credit_distribution = load_application_histogram(filters, "credit_amount")
contract_risk = load_segment_metrics(filters, "contract_type").rename(
    columns={"segment_value": "Contract type"}
)
scatter = load_income_credit_sample(filters)
if not scatter.empty:
    scatter["outcome"] = scatter["TARGET"].map(
        {0: "TARGET=0", 1: "TARGET=1 payment difficulties"}
    )

first_row = st.columns(3)
with first_row[0]:
    render_donut_chart(
        "Application outcome share",
        target_data,
        "Outcome",
        "Applications",
        "Share of filtered applications with TARGET=0 or TARGET=1 payment difficulties.",
    )
with first_row[1]:
    render_bar_chart(
        "Application outcomes",
        target_data,
        "Outcome",
        "Applications",
        "Counts of TARGET=0 and TARGET=1 applications.",
    )
with first_row[2]:
    render_bar_chart(
        "Loan amount distribution",
        credit_distribution,
        "bucket_label",
        "application_count",
        "Application counts by credit amount range.",
    )

second_row = st.columns(2)
with second_row[0]:
    correlation = summary.get("income_credit_correlation")
    render_scatter_chart(
        "Income vs. credit amount (sample)",
        scatter,
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "Sample of application income and requested credit amounts, colored by TARGET.",
        note=(
            "Population Pearson correlation: "
            f"{format_amount(correlation, 3)} (computed across the filtered cohort)."
        ),
    )
with second_row[1]:
    render_bar_chart(
        "TARGET=1 rate by contract type",
        contract_risk,
        "Contract type",
        "target_1_rate",
        "Payment-difficulty rate for each contract type.",
        horizontal=True,
    )
render_data_table(
    "Application details (sample)",
    load_application_sample(filters, "overview"),
    "Sample application records with contract, income, credit, annuity, and TARGET.",
    "application_overview_sample.csv",
)
