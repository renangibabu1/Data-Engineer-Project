import numpy as np
import pandas as pd
import streamlit as st
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from data_access import DashboardFilters, load_prediction_training_sample


@st.cache_resource(show_spinner="Training the exploratory baseline on a sample...")
def train_exploratory_baseline(
    filters: DashboardFilters,
) -> tuple[dict[str, float | int], pd.DataFrame]:
    sample = load_prediction_training_sample(filters)
    if sample.empty or sample["TARGET"].nunique() != 2:
        raise RuntimeError(
            "The application sample must contain both TARGET outcome values."
        )
    if sample["TARGET"].value_counts().min() < 2:
        raise RuntimeError(
            "The application sample needs at least two rows for each TARGET value."
        )

    labels = sample.pop("TARGET").astype("int8")
    applicant_ids = sample.pop("SK_ID_CURR")
    features = sample.copy()
    if "DAYS_EMPLOYED" in features.columns:
        features.loc[features["DAYS_EMPLOYED"] == 365243, "DAYS_EMPLOYED"] = np.nan
    features = features.replace([np.inf, -np.inf], np.nan)

    x_train, x_test, y_train, y_test, _, test_ids = train_test_split(
        features,
        labels,
        applicant_ids,
        test_size=0.25,
        random_state=42,
        stratify=labels,
    )
    model = make_pipeline(
        SimpleImputer(strategy="median", add_indicator=True),
        StandardScaler(),
        LogisticRegression(
            class_weight="balanced",
            max_iter=500,
            random_state=42,
        ),
    )
    model.fit(x_train, y_train)
    scores = model.predict_proba(x_test)[:, 1]
    metrics: dict[str, float | int] = {
        "sample_rows": len(sample),
        "test_rows": len(y_test),
        "target_1_count": int(labels.sum()),
        "roc_auc": float(roc_auc_score(y_test, scores)),
    }
    scored_applicants = pd.DataFrame(
        {
            "SK_ID_CURR": test_ids.to_numpy(),
            "TARGET": y_test.to_numpy(),
            "payment_difficulty_score": scores,
        }
    )
    return metrics, scored_applicants
