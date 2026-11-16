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
)
from data_access import (
    load_application_sample,
    load_application_summary,
    load_segment_metrics,
)


filters = current_filters()
summary = load_application_summary(filters)

st.caption(
    "Compare the source-defined payment-difficulty outcome across applicant "
    "segments. TARGET=1 is not relabeled as general or final loan default."
)
render_metrics(
    [
        (
            "TARGET=1 applications",
            format_count(summary.get("target_1_count")),
            None,
        ),
        (
            "TARGET=1 rate",
            format_percent(summary.get("target_1_rate")),
            "Rate within the current filter selection.",
        ),
        (
            "TARGET=0 applications",
            format_count(summary.get("target_0_count")),
            None,
        ),
        (
            "TARGET=1 average credit",
            format_amount(summary.get("target_1_average_credit")),
            None,
        ),
        (
            "TARGET=1 average income",
            format_amount(summary.get("target_1_average_income")),
            None,
        ),
        (
            "TARGET=1 average annuity",
            format_amount(summary.get("target_1_average_annuity")),
            None,
        ),
    ]
)

outcome_data = pd.DataFrame(
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
segments = (
    ("Gender", "gender"),
    ("Education", "education"),
)
row_one = st.columns(3)
with row_one[0]:
    render_donut_chart(
        "Application outcome share",
        outcome_data,
        "Outcome",
        "Applications",
        "Filtered application counts for TARGET=0 and TARGET=1 payment difficulties.",
    )
for index, (label, dimension) in enumerate(segments):
    data = load_segment_metrics(filters, dimension).rename(
        columns={"segment_value": label}
    )
    with row_one[index + 1]:
        render_bar_chart(
            f"TARGET=1 rate by {label.lower()}",
            data,
            label,
            "target_1_rate",
            f"Payment-difficulty rate by {label.lower()}.",
            horizontal=True,
            height=330,
        )

row_two = st.columns(3)
for index, (label, dimension) in enumerate(
    (
        ("Income band", "income_band"),
        ("Age group", "age_band"),
        ("Contract type", "contract_type"),
    )
):
    data = load_segment_metrics(filters, dimension).rename(
        columns={"segment_value": label}
    )
    with row_two[index]:
        render_bar_chart(
            f"TARGET=1 rate by {label.lower()}",
            data,
            label,
            "target_1_rate",
            f"Payment-difficulty rate by {label.lower()}.",
            horizontal=True,
            height=330,
        )

render_data_table(
    "TARGET=1 application sample",
    load_application_sample(filters, "risk", risk_only=True),
    "Sample of applications with TARGET=1 and selected risk-related fields.",
    "target_1_application_sample.csv",
)
