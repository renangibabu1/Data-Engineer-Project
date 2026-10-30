"""Explore country life expectancy and regional trends from the gold layer."""

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent
GOLD_DIR = PROJECT_ROOT / "gold"

LATEST_FILE = GOLD_DIR / "life_expectancy_latest.parquet"
CHANGE_FILE = GOLD_DIR / "life_expectancy_change_since_2000.parquet"
REGION_YEAR_FILE = GOLD_DIR / "life_expectancy_region_year.parquet"

st.set_page_config(
    page_title="Global life expectancy",
    page_icon=":material/health_metrics:",
    layout="wide",
)


@st.cache_data(ttl="1h", max_entries=1)
def load_dashboard_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load the curated country and regional tables from the gold layer."""
    paths = (LATEST_FILE, CHANGE_FILE, REGION_YEAR_FILE)
    for path in paths:
        if not path.is_file():
            raise FileNotFoundError(
                f"Dashboard data not found: {path}. Run the pipeline to build the gold outputs."
            )

    latest = pd.read_parquet(LATEST_FILE)
    change = pd.read_parquet(CHANGE_FILE)
    region_year = pd.read_parquet(REGION_YEAR_FILE)

    required_columns = (
        (latest, {"country_code", "country_name", "region", "income_group", "year", "value"}),
        (change, {"country_code", "change_years"}),
        (region_year, {"region", "year", "avg_life_expectancy"}),
    )
    for frame, expected in required_columns:
        missing = expected.difference(frame.columns)
        if missing:
            raise ValueError(f"Gold table is missing required columns: {sorted(missing)}")

    return latest, change, region_year


def ranking_chart(data: pd.DataFrame, value_column: str, value_label: str) -> alt.Chart:
    return (
        alt.Chart(data)
        .mark_bar()
        .encode(
            x=alt.X(
                f"{value_column}:Q",
                title=value_label,
                scale=alt.Scale(domainMin=0),
                axis=alt.Axis(format=".1f"),
            ),
            y=alt.Y(
                "country_name:N",
                title=None,
                sort=alt.SortField(field=value_column, order="descending"),
                axis=alt.Axis(labelLimit=160),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="Country"),
                alt.Tooltip(f"{value_column}:Q", title=value_label, format=".1f"),
            ],
        )
        .properties(height=max(220, len(data) * 28))
    )


latest, change, region_year = load_dashboard_data()

regions = sorted(latest["region"].dropna().unique().tolist())
income_groups = sorted(latest["income_group"].dropna().unique().tolist())

st.title("Global life expectancy")
st.caption(
    "Country-level life expectancy and long-run regional trends, built from the project's gold data."
)

with st.sidebar:
    st.header("Filters")
    selected_regions = st.multiselect("Regions", regions, default=regions)
    selected_income_groups = st.multiselect(
        "Income groups", income_groups, default=income_groups
    )
    ranking_size = st.slider("Countries to show", min_value=5, max_value=20, value=10)
    st.divider()
    st.caption(
        "Region and income filters apply to country KPIs, rankings, and the table. "
        "Regional trends use the gold region-year averages."
    )
    with st.expander("About this project"):
        st.markdown("**Technologies used here**")
        st.caption("Python, PySpark, pandas, Delta Lake, Parquet, Streamlit")
        st.markdown("**Related skills**")
        st.caption(
            "Python, PySpark, Azure Data Factory, Databricks, Microsoft Fabric, SQL"
        )

country_scope = latest.loc[
    latest["region"].isin(selected_regions)
    & latest["income_group"].isin(selected_income_groups)
].copy()
change_scope = change.loc[
    change["country_code"].isin(country_scope["country_code"])
].copy()

if country_scope.empty:
    st.info(
        "Choose at least one region and one income group to see country results.",
        title="No countries in this selection",
    )
    st.stop()

average_life_expectancy = country_scope["value"].mean()
average_change = change_scope["change_years"].mean()
latest_year = int(country_scope["year"].max())

metric_columns = st.columns(4, border=True)
metric_columns[0].metric("Avg. expectancy", f"{average_life_expectancy:.1f} yrs")
metric_columns[1].metric(
    "Avg. change",
    f"{average_change:+.1f} yrs" if pd.notna(average_change) else "N/A",
)
metric_columns[2].metric("Countries", f"{len(country_scope):,}")
metric_columns[3].metric("Latest year", str(latest_year))

st.caption(
    "Life expectancy and change are in years; change is since 2000. "
    "Averages use each country's latest available observation, so reporting years may vary."
)

st.subheader("Regional trend")
trend_data = region_year.loc[
    region_year["region"].isin(selected_regions)
].pivot(index="year", columns="region", values="avg_life_expectancy")
if trend_data.empty:
    st.info("Select one or more regions to display the regional trend.")
else:
    st.line_chart(
        trend_data.sort_index(),
        y_label="Life expectancy (years)",
    )

ranking_columns = st.columns(2)
leaders = country_scope.nlargest(ranking_size, "value").sort_values("value")
gains = change_scope.nlargest(ranking_size, "change_years").sort_values("change_years")

with ranking_columns[0]:
    st.subheader("Highest life expectancy")
    st.altair_chart(ranking_chart(leaders, "value", "Life expectancy (years)"))

with ranking_columns[1]:
    st.subheader("Largest gains since 2000")
    if gains.empty:
        st.info("No countries in this selection have comparable data since 2000.")
    else:
        st.altair_chart(ranking_chart(gains, "change_years", "Gain since 2000 (years)"))

st.subheader("Country details")
country_details = country_scope.merge(
    change_scope[["country_code", "change_years"]],
    on="country_code",
    how="left",
    validate="one_to_one",
)
country_details = country_details.rename(
    columns={
        "country_name": "Country",
        "region": "Region",
        "income_group": "Income group",
        "year": "Latest year",
        "value": "Life expectancy (years)",
        "change_years": "Change since 2000 (years)",
    }
)
country_details = country_details[
    [
        "Country",
        "Region",
        "Income group",
        "Latest year",
        "Life expectancy (years)",
        "Change since 2000 (years)",
    ]
].sort_values("Life expectancy (years)", ascending=False)

st.dataframe(
    country_details,
    hide_index=True,
)