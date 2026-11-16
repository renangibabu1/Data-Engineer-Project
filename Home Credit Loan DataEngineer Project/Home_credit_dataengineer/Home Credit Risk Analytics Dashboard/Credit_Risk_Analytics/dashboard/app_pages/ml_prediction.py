import pandas as pd
import streamlit as st
from sklearn.metrics import precision_score, recall_score

from common import (
    current_filters,
    format_count,
    format_percent,
    render_bar_chart,
    render_metrics,
)
from modeling import train_exploratory_baseline


st.warning(
    "Exploratory prototype only. This sampled baseline is not validated for "
    "loan decisions and must not be used to approve or reject applicants. "
    "The source-defined TARGET=1 outcome means payment difficulties."
)

filters = current_filters()
if filters.target is not None:
    st.info("Set the TARGET outcome filter to All to train and evaluate both classes.")
    st.stop()

metrics, scored_applicants = train_exploratory_baseline(filters)
threshold = st.slider(
    "Exploratory score threshold",
    min_value=0.10,
    max_value=0.90,
    value=0.50,
    step=0.05,
    help="Used only to illustrate score precision and recall on the held-out sample.",
)
predicted_positive = scored_applicants["payment_difficulty_score"] >= threshold
precision = precision_score(
    scored_applicants["TARGET"], predicted_positive, zero_division=0
)
recall = recall_score(
    scored_applicants["TARGET"], predicted_positive, zero_division=0
)

render_metrics(
    [
        ("Training sample rows", format_count(metrics["sample_rows"]), None),
        ("Held-out applicants", format_count(metrics["test_rows"]), None),
        ("Sample TARGET=1 rows", format_count(metrics["target_1_count"]), None),
        ("Held-out ROC AUC", f"{metrics['roc_auc']:.3f}", None),
        ("Precision at threshold", format_percent(precision), None),
        ("Recall at threshold", format_percent(recall), None),
    ]
)

score_bins = pd.cut(
    scored_applicants["payment_difficulty_score"],
    bins=[index / 10 for index in range(11)],
    include_lowest=True,
)
distribution = (
    scored_applicants.assign(score_band=score_bins)
    .groupby(["score_band", "TARGET"], observed=False)
    .size()
    .unstack(fill_value=0)
    .rename(columns={0: "TARGET=0", 1: "TARGET=1"})
    .reset_index()
)
distribution["score_band"] = distribution["score_band"].astype(str)
render_bar_chart(
    "Held-out applicants by exploratory score band",
    distribution.melt(
        id_vars="score_band",
        var_name="TARGET outcome",
        value_name="applicant_count",
    ),
    "score_band",
    "applicant_count",
    "Held-out applicants grouped by model score band and observed TARGET value.",
    color="TARGET outcome",
)

st.subheader("Highest-scoring held-out sample")
st.dataframe(
    scored_applicants.sort_values(
        "payment_difficulty_score", ascending=False
    ).head(50),
    hide_index=True,
    width="stretch",
)
