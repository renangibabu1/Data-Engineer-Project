import streamlit as st

from common import (
    current_filters,
    format_amount,
    format_percent,
    render_bar_chart,
    render_data_table,
    render_donut_chart,
    render_metrics,
)
from data_access import (
    load_application_histogram,
    load_application_sample,
    load_application_summary,
    load_contract_credit_totals,
    load_ratio_by_target,
    load_segment_metrics,
)


filters = current_filters()
summary = load_application_summary(filters)
application_count = summary.get("application_count") or 0
cash_loan_share = (
    (summary.get("cash_loan_count") or 0) / application_count
    if application_count
    else None
)

st.caption(
    "Analyze requested loan amounts, contract types, and affordability ratios "
    "for the filtered application cohort."
)
render_metrics(
    [
        ("Total credit amount", format_amount(summary.get("total_credit_amount")), None),
        (
            "Average credit amount",
            format_amount(summary.get("average_credit_amount")),
            None,
        ),
        (
            "Median credit amount",
            format_amount(summary.get("median_credit_amount")),
            None,
        ),
        (
            "Average credit-to-income ratio",
            format_amount(summary.get("average_loan_to_income_ratio"), 2),
            None,
        ),
        (
            "Average annuity-to-income ratio",
            format_amount(summary.get("average_annuity_to_income_ratio"), 2),
            None,
        ),
        ("Cash-loan share", format_percent(cash_loan_share), None),
        (
            "Average goods price",
            format_amount(summary.get("average_goods_price"), 2),
            None,
        ),
    ]
)

credit_distribution = load_application_histogram(filters, "credit_amount")
contract_credit = load_contract_credit_totals(filters).rename(
    columns={"NAME_CONTRACT_TYPE": "Contract type"}
)
loan_to_income = load_application_histogram(filters, "loan_to_income")
annuity_comparison = load_ratio_by_target(filters).rename(
    columns={"target_group": "Outcome"}
)
contract_share = load_segment_metrics(filters, "contract_type").rename(
    columns={"segment_value": "Contract type"}
)

row_one = st.columns(2)
with row_one[0]:
    render_bar_chart(
        "Loan amount distribution",
        credit_distribution,
        "bucket_label",
        "application_count",
        "Application counts by requested credit amount range.",
    )
with row_one[1]:
    render_bar_chart(
        "Total credit by contract type",
        contract_credit,
        "Contract type",
        "total_credit_amount",
        "Total requested credit amount by contract type.",
    )

row_two = st.columns(3)
with row_two[0]:
    render_bar_chart(
        "Applications by credit-to-income range",
        loan_to_income,
        "bucket_label",
        "application_count",
        "Application counts by credit-to-income ratio range.",
    )
with row_two[1]:
    render_bar_chart(
        "Annuity-to-income ratio by outcome",
        annuity_comparison,
        "Outcome",
        "average_annuity_to_income_ratio",
        "Average annuity-to-income ratio for TARGET=0 and TARGET=1 applications.",
    )
with row_two[2]:
    render_donut_chart(
        "Contract type share",
        contract_share,
        "Contract type",
        "application_count",
        "Filtered application counts by contract type.",
    )

render_data_table(
    "Loan application details (sample)",
    load_application_sample(filters, "loan"),
    "Sample loan applications with requested credit, income, annuity, goods price, and TARGET.",
    "loan_application_sample.csv",
)
