from pathlib import Path

import pandas as pd

from agents.state import UnderwritingState
from model.explain import calculate_shap, format_shap_values, take_n_features
from model.train import load_model


def risk_assessment(state: UnderwritingState) -> UnderwritingState:
    model_path = Path("model/artifacts/lgbm_model.joblib")

    model = load_model(model_path)

    df = pd.DataFrame([state.features])

    prob = model.predict_proba(df)[:, 1][0]

    shap_values = calculate_shap(model, df)

    n_pos_shap_values, n_neg_shap_values = take_n_features(
        shap_values[0],
        df.columns,
        10,
    )

    shap_prompt = format_shap_values(
        n_pos_shap_values,
        n_neg_shap_values,
        1 if prob > 0.5 else 0,
    )

    return state.model_copy(
        update={"probability_of_default": prob, "shap_summary": shap_prompt},
    )
