import streamlit as st

from common import (
    current_filters,
    format_amount,
    format_count,
    format_percent,
    render_bar_chart,
    render_data_table,
    render_donut_chart,
    render_line_chart,
    render_metrics,
)
from data_access import (
    load_application_histogram,
    load_application_sample,
    load_application_summary,
    load_segment_metrics,
)


filters = current_filters()
summary = load_application_summary(filters)
application_count = summary.get("application_count", 0)
married_share = (
    (summary.get("married_application_count") or 0) / application_count
    if application_count
    else None
)
children_share = (
    (summary.get("customers_with_children_count") or 0) / application_count
    if application_count
    else None
)

st.caption(
    "Explore age, gender, family structure, and education. "
    "TARGET=1 is payment difficulties, not a confirmed final default."
)
render_metrics(
    [
        ("Male applicants", format_count(summary.get("male_application_count")), None),
        (
            "Female applicants",
            format_count(summary.get("female_application_count")),
            None,
        ),
        ("Average age", f"{format_amount(summary.get('average_age_years'), 1)} years", None),
        ("Married applicants", format_percent(married_share), None),
        ("Applicants with children", format_percent(children_share), None),
        (
            "Average family size",
            format_amount(summary.get("average_family_size"), 2),
            None,
        ),
    ]
)

age_distribution = load_application_histogram(filters, "age_years")
gender_distribution = load_segment_metrics(filters, "gender").rename(
    columns={"segment_value": "Gender"}
)
family_distribution = load_segment_metrics(filters, "family_status").rename(
    columns={"segment_value": "Family status"}
)
education_risk = load_segment_metrics(filters, "education").rename(
    columns={"segment_value": "Education"}
)
age_risk = load_segment_metrics(filters, "age_band").rename(
    columns={"segment_value": "Age group"}
)

row_one = st.columns(3)
with row_one[0]:
    render_bar_chart(
        "Age distribution",
        age_distribution,
        "bucket_label",
        "application_count",
        "Application counts by five-year age range.",
    )
with row_one[1]:
    render_donut_chart(
        "Applicant gender share",
        gender_distribution,
        "Gender",
        "application_count",
        "Filtered application counts by gender.",
    )
with row_one[2]:
    render_bar_chart(
        "Applicants by family status",
        family_distribution,
        "Family status",
        "application_count",
        "Filtered application counts by family status.",
    )

row_two = st.columns(2)
with row_two[0]:
    render_bar_chart(
        "TARGET=1 rate by education",
        education_risk,
        "Education",
        "target_1_rate",
        "Payment-difficulty rate by education level.",
        horizontal=True,
    )
with row_two[1]:
    render_line_chart(
        "TARGET=1 rate by age group",
        age_risk,
        "Age group",
        "target_1_rate",
        "Payment-difficulty rate across age bands in natural age order.",
        color="#8E6CBE",
    )
render_data_table(
    "Customer demographics (sample)",
    load_application_sample(filters, "demographics"),
    "Sample applicant demographics and TARGET values.",
    "customer_demographics_sample.csv",
)
