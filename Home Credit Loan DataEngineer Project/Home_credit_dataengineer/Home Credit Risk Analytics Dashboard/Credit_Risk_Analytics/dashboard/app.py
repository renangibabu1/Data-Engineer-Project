from pathlib import Path

import streamlit as st

from data_access import DashboardFilters


APP_DIRECTORY = Path(__file__).resolve().parent
FILTER_KEYS = (
    "filter_contract_type",
    "filter_gender",
    "filter_income_range",
    "filter_credit_range",
    "filter_target",
)


def reset_filters() -> None:
    for key in FILTER_KEYS:
        st.session_state.pop(key, None)


st.set_page_config(
    page_title="Home Credit Risk Analytics",
    page_icon=":material/analytics:",
    layout="wide",
)
st.html(APP_DIRECTORY / "assets" / "dashboard.css")

with st.sidebar.container(key="sidebar-brand"):
    st.html(
        """
        <div class="brand-lockup">
          <div class="brand-mark">HC</div>
          <div><strong>Home Credit</strong><span>Risk Analytics</span></div>
        </div>
        """
    )

pages = [
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "executive_overview.py"),
        title="Executive Overview",
        icon=":material/home:",
        help="Portfolio KPIs, application outcomes, loan amounts, and contract comparisons.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "customer_demographics.py"),
        title="Customer Demographics",
        icon=":material/group:",
        help="Explore applicant age, gender, family structure, and education.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "loan_application.py"),
        title="Loan Application Analysis",
        icon=":material/bar_chart:",
        help="Compare requested credit, contract types, and affordability ratios.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "risk_analysis.py"),
        title="Risk Analysis",
        icon=":material/warning:",
        help="Compare the source-defined TARGET=1 payment-difficulty rate across segments.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "income_employment.py"),
        title="Income & Employment",
        icon=":material/work:",
        help="Review income bands, occupation, and recorded employment tenure.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "credit_bureau.py"),
        title="Credit Bureau Analysis",
        icon=":material/account_balance:",
        help="Analyze external bureau records, credit status, overdue balances, and credit age.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "previous_applications.py"),
        title="Previous Loan Applications",
        icon=":material/history:",
        help="Explore prior loan decisions, requested amounts, and application purposes.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "repayment_installments.py"),
        title="Repayment & Installments",
        icon=":material/payments:",
        help="Review installment payment timing, amounts, and recorded shortfalls.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "ml_prediction.py"),
        title="ML Payment-Difficulty Explorer",
        icon=":material/model_training:",
        help="Explore a sampled, non-decisioning baseline for TARGET=1 payment difficulties.",
    ),
    st.Page(
        str(APP_DIRECTORY / "app_pages" / "reports_download.py"),
        title="Reports & Data Download",
        icon=":material/download:",
        help="Download bounded, filtered report samples as CSV files.",
    ),
]
page = st.navigation(pages, position="hidden")

with st.sidebar.container(key="sidebar-navigation"):
    for nav_page in pages:
        st.page_link(
            nav_page,
            label=nav_page.title,
            icon=nav_page.icon,
            help=nav_page.help,
        )

if st.sidebar.button("Refresh data", key="refresh_data"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

with st.sidebar.container(key="dataset-meta", border=True):
    st.markdown("**Dataset**")
    st.caption("application_train.csv · 307,511 rows · 122 columns")
    st.caption("Databricks Silver + Gold")

with st.container(key="dashboard-banner"):
    st.html(
        f"""
        <div class="banner-content">
          <div>
            <h1>
              <span class="dashboard-title-brand">Home Credit</span>
              <span class="dashboard-title-separator">|</span>
              <span>{page.title}</span>
            </h1>
            <p>Training cohort · TARGET=1 denotes recorded payment difficulties, not confirmed final default.</p>
          </div>
          <div class="dashboard-meta">
            <span class="dashboard-badge">Dataset: application_train.csv</span>
            <span class="dashboard-count">Applications: 307,511</span>
          </div>
        </div>
        """
    )

with st.container(key="dashboard-filter-toolbar", border=True):
    st.markdown("#### Portfolio filters")
    with st.form("portfolio_filters", border=False):
        with st.grid(
            6,
            min_column_width=145,
            gap="xsmall",
            vertical_alignment="bottom",
        ):
            contract_type = st.selectbox(
                "Contract type",
                ("All", "Cash loans", "Revolving loans"),
                key="filter_contract_type",
            )
            gender = st.selectbox(
                "Gender",
                ("All", "F", "M", "XNA"),
                key="filter_gender",
            )
            income_range = st.slider(
                "Income range",
                min_value=0,
                max_value=120_000_000,
                value=(0, 120_000_000),
                step=25_000,
                key="filter_income_range",
            )
            credit_range = st.slider(
                "Credit amount range",
                min_value=0,
                max_value=5_000_000,
                value=(0, 5_000_000),
                step=25_000,
                key="filter_credit_range",
            )
            target_label = st.selectbox(
                "TARGET outcome",
                ("All", "TARGET=0", "TARGET=1 payment difficulties"),
                key="filter_target",
            )
            with st.container(horizontal=True, gap="xxsmall"):
                st.form_submit_button(
                    "Apply filters",
                    key="apply_filters",
                    icon=":material/filter_alt:",
                    type="primary",
                )
                st.form_submit_button(
                    "Reset filters",
                    key="reset_filters",
                    icon=":material/restart_alt:",
                    on_click=reset_filters,
                )

target = {
    "All": None,
    "TARGET=0": 0,
    "TARGET=1 payment difficulties": 1,
}[target_label]
st.session_state["dashboard_filters"] = DashboardFilters(
    contract_type=contract_type,
    gender=gender,
    income_min=income_range[0],
    income_max=income_range[1],
    credit_min=credit_range[0],
    credit_max=credit_range[1],
    target=target,
)

page.run()
