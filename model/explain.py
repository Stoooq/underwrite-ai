import lightgbm as lgb
import pandas as pd
import shap


def calculate_shap(model: lgb.LGBMClassifier, df: pd.DataFrame) -> shap.Explanation:
    explainer = shap.Explainer(model)

    shap_values = explainer(df)

    return shap_values


def take_n_features(
    shap_values: shap.Explanation,
    columns: list[str],
    n: int,
) -> tuple[pd.Series, pd.Series]:
    values = shap_values.values

    if values.ndim == 2:
        values = values.mean(axis=0)

    feature_importance = pd.Series(values, index=columns)

    pos_feature_importance = feature_importance[feature_importance > 0].sort_values(
        ascending=False,
    )
    neg_feature_importance = feature_importance[feature_importance < 0].sort_values()

    return pos_feature_importance[:n], neg_feature_importance[:n]


def format_shap_values(
    n_pos_shap_values: pd.Series,
    n_neg_shap_values: pd.Series,
    target: int,
) -> str:
    pos_prompt = "Features that INCREASED the risk of default:\n" + "\n".join(
        f"  {name}: {value:.4f}" for name, value in n_pos_shap_values.items()
    )
    neg_prompt = "Features that DECREASED the risk of default: \n" + "\n".join(
        f"  {name}: {value:.4f}" for name, value in n_neg_shap_values.items()
    )

    shap_prompt = f"The model predicted that this client will {'DEFAULT' if target == 1 else 'REPAY'} on their loan (TARGET={target}). \n{pos_prompt}, \n{neg_prompt}"

    return shap_prompt
