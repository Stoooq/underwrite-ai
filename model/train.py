from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import confusion_matrix, roc_auc_score
from sklearn.model_selection import train_test_split


def load_parquet_data(data_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(data_path)

    return df


def split_data(
    df: pd.DataFrame, target_col: str, val_size: float = 0.2, random_state: int = 42
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Series, pd.Series]:
    X = df.loc[:, df.columns != target_col]
    y = df[target_col]

    X_train, X_val, y_train, y_val = train_test_split(
        X,
        y,
        test_size=val_size,
        random_state=random_state,
    )

    return X_train, X_val, y_train, y_val


def train_model(
    data_path: Path,
    target_col: str,
    val_size: float,
    model: lgb.LGBMClassifier,
    random_state: int = 42,
) -> tuple[float, float, np.ndarray]:
    df = load_parquet_data(data_path)

    X_train, X_val, y_train, y_val = split_data(
        df,
        target_col,
        val_size,
        random_state=random_state,
    )

    model.fit(X_train, y_train)

    y_pred_prob = model.predict_proba(X_val)[:, 1]
    y_pred = model.predict(X_val)

    acc = np.sum(y_pred == y_val) / len(y_val)

    roc_auc = roc_auc_score(y_val, y_pred_prob)

    conf_matrix = confusion_matrix(y_val, y_pred)

    return acc, roc_auc, conf_matrix
