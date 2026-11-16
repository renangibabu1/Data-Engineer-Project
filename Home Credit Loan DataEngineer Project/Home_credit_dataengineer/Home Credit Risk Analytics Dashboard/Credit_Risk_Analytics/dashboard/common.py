from collections.abc import Sequence

import pandas as pd
import streamlit as st

from data_access import DashboardFilters


Metric = tuple[str, str, str | None]


def current_filters() -> DashboardFilters:
    filters = st.session_state.get("dashboard_filters")
    if not isinstance(filters, DashboardFilters):
        raise RuntimeError("Dashboard filters were not initialized.")
    return filters


def format_count(value: object) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{int(value):,}"


def format_amount(value: object, decimals: int = 0) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):,.{decimals}f}"


def format_percent(value: object, decimals: int = 1) -> str:
    if value is None or pd.isna(value):
        return "N/A"
    return f"{float(value):.{decimals}%}"


def render_metrics(metrics: Sequence[Metric]) -> None:
    with st.grid("auto", min_column_width=160, gap="small"):
        for index, (label, value, help_text) in enumerate(metrics):
            with st.container(key=f"kpi-card-{index}", border=True):
                st.metric(label, value, help=help_text)


def render_donut_chart(
    title: str,
    data: pd.DataFrame,
    category: str,
    value: str,
    alt: str,
    *,
    height: int = 280,
) -> None:
    with st.container(border=True):
        st.subheader(title)
        if data.empty:
            st.info("No rows match the current filters.")
            return
        st.vega_lite_chart(
            data,
            {
                "mark": {
                    "type": "arc",
                    "innerRadius": 58,
                    "outerRadius": 96,
                    "stroke": "#ffffff",
                },
                "encoding": {
                    "theta": {
                        "field": value,
                        "type": "quantitative",
                        "aggregate": "sum",
                    },
                    "color": {
                        "field": category,
                        "type": "nominal",
                        "legend": {"orient": "bottom"},
                    },
                    "tooltip": [
                        {"field": category, "type": "nominal"},
                        {
                            "field": value,
                            "type": "quantitative",
                            "format": ",.0f",
                        },
                    ],
                },
                "view": {"stroke": None},
            },
            width="stretch",
            height=height,
            alt=alt,
        )


def render_line_chart(
    title: str,
    data: pd.DataFrame,
    x: str,
    y: str,
    alt: str,
    *,
    color: str | None = None,
    height: int = 300,
) -> None:
    with st.container(border=True):
        st.subheader(title)
        if data.empty:
            st.info("No rows match the current filters.")
            return
        st.line_chart(
            data,
            x=x,
            y=y,
            color=color,
            height=height,
            width="stretch",
            alt=alt,
        )


def render_bar_chart(
    title: str,
    data: pd.DataFrame,
    x: str,
    y: str,
    alt: str,
    *,
    horizontal: bool = False,
    height: int = 300,
    color: str | None = None,
) -> None:
    with st.container(border=True):
        st.subheader(title)
        if data.empty:
            st.info("No rows match the current filters.")
            return
        st.bar_chart(
            data,
            x=x,
            y=y,
            horizontal=horizontal,
            color=color,
            sort=False,
            height=height,
            width="stretch",
            alt=alt,
        )


def render_data_table(
    title: str,
    data: pd.DataFrame,
    alt: str,
    file_name: str,
    *,
    percent_columns: tuple[str, ...] = (),
) -> None:
    with st.container(border=True):
        st.subheader(title)
        if data.empty:
            st.info("No rows match the current filters.")
            return
        column_config = {
            column: st.column_config.NumberColumn(format="percent")
            for column in percent_columns
            if column in data.columns
        }
        st.dataframe(
            data,
            hide_index=True,
            height=380,
            width="stretch",
            column_config=column_config,
            alt=alt,
        )
        st.download_button(
            "Download sample CSV",
            data=data.to_csv(index=False).encode("utf-8"),
            file_name=file_name,
            mime="text/csv",
            icon=":material/download:",
        )


def render_scatter_chart(
    title: str,
    data: pd.DataFrame,
    x: str,
    y: str,
    alt: str,
    *,
    note: str | None = None,
) -> None:
    with st.container(border=True):
        st.subheader(title)
        if data.empty:
            st.info("No rows match the current filters.")
            return
        if note:
            st.caption(note)
        st.scatter_chart(
            data,
            x=x,
            y=y,
            color="outcome",
            width="stretch",
            height=300,
            alt=alt,
        )
