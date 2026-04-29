import os
from pathlib import Path

from pyspark.sql import SparkSession

from spark.aggregations import (
    aggregate_bureau,
    aggregate_bureau_balance,
    aggregate_credit_card_balance,
    aggregate_installments,
    aggregate_pos_cash_balance,
    aggregate_previous_application,
)
from spark.ingestion import DataLoader
from spark.schemas import (
    APPLICATION_SCHEMA,
    BUREAU_BALANCE_SCHEMA,
    BUREAU_SCHEMA,
    CREDIT_CARD_BALANCE_SCHEMA,
    INSTALLMENTS_PAYMENTS_SCHEMA,
    POS_CASH_BALANCE_SCHEMA,
    PREVIOUS_APPLICATION_SCHEMA,
)


def main():
    os.environ["JAVA_HOME"] = "/usr/local/opt/openjdk@17"
    spark = SparkSession.builder.getOrCreate()

    data_path = Path("data/raw/home-credit-default-risk")

    dl = DataLoader(spark, base_path=data_path)

    data_schemas = [
        ("application", "application_train.csv", APPLICATION_SCHEMA),
        ("bureau", "bureau.csv", BUREAU_SCHEMA),
        ("bureau_balance", "bureau_balance.csv", BUREAU_BALANCE_SCHEMA),
        (
            "previous_application",
            "previous_application.csv",
            PREVIOUS_APPLICATION_SCHEMA,
        ),
        (
            "installments_payments",
            "installments_payments.csv",
            INSTALLMENTS_PAYMENTS_SCHEMA,
        ),
        ("pos_cash_balance", "POS_CASH_balance.csv", POS_CASH_BALANCE_SCHEMA),
        ("credit_card_balance", "credit_card_balance.csv", CREDIT_CARD_BALANCE_SCHEMA),
    ]

    dataframes = {}

    for name, path, schema in data_schemas:
        df = dl.load_table(path=path, schema=schema)
        dataframes[name] = df

    df_bureau_balance = aggregate_bureau_balance(dataframes["bureau_balance"])
    df_aggregated = dataframes["bureau"].join(df_bureau_balance, "SK_ID_BUREAU", "left")
    df_bureau = aggregate_bureau(df_aggregated)

    df_prev_app = aggregate_previous_application(dataframes["previous_application"])

    df_inst = aggregate_installments(dataframes["installments_payments"])

    df_pos = aggregate_pos_cash_balance(dataframes["pos_cash_balance"])

    df_cc = aggregate_credit_card_balance(dataframes["credit_card_balance"])

    df_feature_store = (
        dataframes["application"]
        .join(df_bureau, "SK_ID_CURR", "left")
        .join(df_prev_app, "SK_ID_CURR", "left")
        .join(df_inst, "SK_ID_CURR", "left")
        .join(df_pos, "SK_ID_CURR", "left")
        .join(df_cc, "SK_ID_CURR", "left")
    )

    df_feature_store.show(vertical=True)

    spark.stop()


if __name__ == "__main__":
    main()
