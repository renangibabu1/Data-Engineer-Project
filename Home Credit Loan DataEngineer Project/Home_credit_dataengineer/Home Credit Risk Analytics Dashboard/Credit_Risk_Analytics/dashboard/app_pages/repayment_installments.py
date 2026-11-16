import streamlit as st

from common import (
    current_filters,
    format_amount,
    format_count,
    render_bar_chart,
    render_data_table,
    render_metrics,
)
from data_access import (
    load_installments_sample,
    load_installments_status_metrics,
    load_installments_summary,
)


filters = current_filters()
summary = load_installments_summary(filters)

st.caption(
    "An installment is labeled late when DAYS_ENTRY_PAYMENT is greater than "
    "DAYS_INSTALMENT. Rows without recorded payment date/amount are shown "
    "separately; the shortfall calculation treats missing AMT_PAYMENT as zero "
    "and does not prove that no payment occurred."
)
render_metrics(
    [
        ("Installment records", format_count(summary.get("installment_count")), None),
        ("Customers", format_count(summary.get("customer_count")), None),
        ("Scheduled amount", format_amount(summary.get("scheduled_amount")), None),
        ("Recorded paid amount", format_amount(summary.get("paid_amount")), None),
        ("Recorded shortfall", format_amount(summary.get("payment_shortfall")), None),
        (
            "Late installments",
            format_count(summary.get("late_installment_count")),
            None,
        ),
    ]
)

status_metrics = load_installments_status_metrics(filters).rename(
    columns={"payment_status": "Payment status"}
)
chart_row = st.columns(2)
with chart_row[0]:
    render_bar_chart(
        "Installments by payment timing",
        status_metrics,
        "Payment status",
        "installment_count",
        "Installment records grouped by recorded payment timing.",
    )
with chart_row[1]:
    render_bar_chart(
        "Recorded payment shortfall by timing",
        status_metrics,
        "Payment status",
        "payment_shortfall",
        "Scheduled-minus-paid amount, floored at zero, grouped by payment timing.",
    )

render_data_table(
    "Repayment and installment records (sample)",
    load_installments_sample(filters),
    "A bounded sample of installment records for the filtered applicants.",
    "installment_sample.csv",
)
