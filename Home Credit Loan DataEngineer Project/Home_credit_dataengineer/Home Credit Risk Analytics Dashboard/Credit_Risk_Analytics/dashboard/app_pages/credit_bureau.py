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
)
from data_access import (
    load_application_summary,
    load_bureau_duration_distribution,
    load_bureau_education_metrics,
    load_bureau_overdue_distribution,
    load_bureau_sample,
    load_bureau_status_metrics,
    load_bureau_summary,
)


filters = current_filters()
summary = load_bureau_summary(filters)
application_summary = load_application_summary(filters)
application_count = application_summary.get("application_count") or 0
bureau_customer_count = summary.get("customer_count") or 0
bureau_link_share = (
    bureau_customer_count / application_count if application_count else None
)

st.caption(
    "External credit records linked to the filtered application cohort. "
    "Record-level totals are distinct from unique applicants; credit age uses "
    "the absolute value of DAYS_CREDIT."
)
render_metrics(
    [
        (
            "Applicants with bureau history",
            format_count(bureau_customer_count),
            f"{format_percent(bureau_link_share)} of the filtered application cohort.",
        ),
        (
            "Average total bureau credit",
            format_amount(summary.get("average_customer_bureau_credit")),
            "Average summed AMT_CREDIT_SUM per linked applicant where reported.",
        ),
        (
            "Active bureau records",
            format_count(summary.get("active_credit_count")),
            None,
        ),
        (
            "Total overdue amount",
            format_amount(summary.get("total_overdue_amount")),
            None,
        ),
        (
            "Average credit age",
            f"{format_amount(summary.get('average_credit_age_days'), 0)} days",
            None,
        ),
        (
            "TARGET=1 rate, bureau-linked",
            format_percent(summary.get("target_1_rate")),
            (
                "Observed payment-difficulty rate for applicants with bureau history. "
                f"Overall filtered rate: {format_percent(application_summary.get('target_1_rate'))}."
            ),
        ),
    ]
)

status_metrics = load_bureau_status_metrics(filters).rename(
    columns={"credit_status": "Credit status"}
)
overdue_distribution = load_bureau_overdue_distribution(filters).rename(
    columns={"overdue_bucket": "Overdue amount"}
)
duration_distribution = load_bureau_duration_distribution(filters).rename(
    columns={"credit_age_bucket": "Credit age (days)"}
)
education_metrics = load_bureau_education_metrics(filters).rename(
    columns={"education": "Education"}
)

row_one = st.columns(3)
with row_one[0]:
    render_donut_chart(
        "Credit active-status composition",
        status_metrics,
        "Credit status",
        "credit_count",
        "Bureau credit record counts by all recorded active statuses.",
    )
with row_one[1]:
    render_bar_chart(
        "Bureau credit records by status",
        status_metrics,
        "Credit status",
        "credit_count",
        "Number of bureau records by active or closed status.",
    )
with row_one[2]:
    render_bar_chart(
        "Overdue amount distribution",
        overdue_distribution,
        "Overdue amount",
        "credit_count",
        "Bureau records grouped by overdue amount.",
    )

row_two = st.columns(2)
with row_two[0]:
    render_bar_chart(
        "Credit age distribution",
        duration_distribution,
        "Credit age (days)",
        "credit_count",
        "Bureau records grouped by absolute days before the application.",
    )
with row_two[1]:
    render_bar_chart(
        "Average bureau credit by education",
        education_metrics,
        "Education",
        "average_bureau_credit",
        "Average total bureau credit per applicant by education level.",
        horizontal=True,
    )

render_data_table(
    "Bureau credit records (sample)",
    load_bureau_sample(filters),
    "Sample external credit records linked to the filtered applicants.",
    "bureau_credit_sample.csv",
)
