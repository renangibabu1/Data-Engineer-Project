import streamlit as st

from common import (
    current_filters,
    format_amount,
    format_count,
    format_percent,
    render_bar_chart,
    render_data_table,
    render_metrics,
)
from data_access import (
    load_previous_application_sample,
    load_previous_application_summary,
    load_previous_purpose_metrics,
    load_previous_status_metrics,
)


filters = current_filters()
summary = load_previous_application_summary(filters)

st.caption(
    "Prior-application counts are application records, not unique customers. "
    "Approval rate compares approved records with approved plus refused records."
)
render_metrics(
    [
        ("Previous applications", format_count(summary.get("previous_application_count")), None),
        ("Customers with history", format_count(summary.get("customer_count")), None),
        ("Approved applications", format_count(summary.get("approved_count")), None),
        ("Refused applications", format_count(summary.get("refused_count")), None),
        ("Approval rate", format_percent(summary.get("approval_rate")), None),
        (
            "Average requested amount",
            format_amount(summary.get("average_requested_amount")),
            None,
        ),
    ]
)

status_metrics = load_previous_status_metrics(filters).rename(
    columns={"application_status": "Application status"}
)
purpose_metrics = load_previous_purpose_metrics(filters).rename(
    columns={"loan_purpose": "Loan purpose"}
)

chart_row = st.columns(2)
with chart_row[0]:
    render_bar_chart(
        "Previous applications by decision status",
        status_metrics,
        "Application status",
        "application_count",
        "Previous loan application records grouped by contract status.",
    )
with chart_row[1]:
    render_bar_chart(
        "Most common previous loan purposes",
        purpose_metrics,
        "Loan purpose",
        "application_count",
        "Previous loan application records grouped by requested loan purpose.",
        horizontal=True,
    )

render_data_table(
    "Previous loan application records (sample)",
    load_previous_application_sample(filters),
    "A bounded sample of prior loan applications for the filtered applicants.",
    "previous_application_sample.csv",
)
