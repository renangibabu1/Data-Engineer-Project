import streamlit as st

from common import current_filters, render_data_table
from data_access import (
    load_application_sample,
    load_installments_sample,
    load_previous_application_sample,
)


filters = current_filters()
report_type = st.selectbox(
    "Choose a report sample",
    (
        "Current application cohort",
        "Previous loan applications",
        "Repayment installments",
    ),
)

if report_type == "Current application cohort":
    data = load_application_sample(filters, "overview")
    filename = "application_cohort_sample.csv"
    description = (
        "A bounded sample of current training-cohort applications matching the "
        "global filters. TARGET=1 means payment difficulties."
    )
elif report_type == "Previous loan applications":
    data = load_previous_application_sample(filters)
    filename = "previous_application_sample.csv"
    description = (
        "A bounded sample of previous loan application records linked to "
        "applicants matching the global filters."
    )
else:
    data = load_installments_sample(filters)
    filename = "installment_sample.csv"
    description = (
        "A bounded sample of repayment installment records linked to applicants "
        "matching the global filters."
    )

st.info(
    "Downloads are bounded to 100 rows to keep browser exports responsive. "
    "Use Databricks for full-table extracts."
)
render_data_table(
    f"{report_type} — filtered sample",
    data,
    description,
    filename,
)
