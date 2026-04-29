from pyspark.ml.feature import StringIndexer
from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def encode_categoricals(df: DataFrame, columns: list[str]) -> DataFrame:
    outputs = [column.lower() + "_index" for column in columns]
    string_indexer = StringIndexer(inputCols=columns, outputCols=outputs, handleInvalid="keep")

    model = string_indexer.fit(df)
    result = model.transform(df)

    result = result.drop(*columns)

    return result


def transform_time_columns(df: DataFrame) -> DataFrame:
    result = df.withColumns(
        {
            "age_years": sf.abs(df["DAYS_BIRTH"] / 365),
            "employment_years": sf.abs(df["DAYS_EMPLOYED"] / 365),
            "is_unemployed": sf.when(df["DAYS_EMPLOYED"] == 365243, 1).otherwise(0),
        },
    )

    result = result.drop("DAYS_BIRTH", "DAYS_EMPLOYED")

    return result


def add_derived_features(df: DataFrame) -> DataFrame:
    result = df.withColumns(
        {
            "credit_to_income_ratio": df["AMT_CREDIT"] / df["AMT_INCOME_TOTAL"],
            "annuity_to_income_ratio": df["AMT_ANNUITY"] / df["AMT_INCOME_TOTAL"],
            "credit_to_goods_ratio": df["AMT_CREDIT"] / df["AMT_GOODS_PRICE"],
            "prev_app_refusal_rate": sf.when(
                df["prev_app_total_count"] > 0,
                df["prev_app_refused_count"] / df["prev_app_total_count"],
            ).otherwise(None),
            "bureau_debt_to_credit_ratio": sf.when(
                df["bureau_sum_credit"] > 0,
                df["bureau_sum_debt"] / df["bureau_sum_credit"],
            ).otherwise(None),
        },
    )

    return result


def run_feature_engineering(df: DataFrame, categorical_columns: list[str]) -> DataFrame:
    df = encode_categoricals(df, categorical_columns)

    df = transform_time_columns(df)

    df = add_derived_features(df)

    return df
