import os
from datetime import datetime
from pathlib import Path

import lightgbm as lgb
from pyspark.sql import SparkSession

from model.train import train_model
from spark.aggregations import (
    aggregate_bureau,
    aggregate_bureau_balance,
    aggregate_credit_card_balance,
    aggregate_installments,
    aggregate_pos_cash_balance,
    aggregate_previous_application,
)
from spark.feature_engineering import run_feature_engineering
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
    spark = SparkSession.builder.config("spark.driver.memory", "2g").getOrCreate()

    raw_data_path = Path("data/raw/home-credit-default-risk")
    processed_data_path = (
        Path("data/processed") / f"features_{datetime.now().strftime('%Y-%m-%d')}"
    )

    if not processed_data_path.exists():
        dl = DataLoader(spark, base_path=raw_data_path)

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
            (
                "credit_card_balance",
                "credit_card_balance.csv",
                CREDIT_CARD_BALANCE_SCHEMA,
            ),
        ]

        dataframes = {}

        for name, path, schema in data_schemas:
            df = dl.load_table(path=path, schema=schema)
            dataframes[name] = df

        # print(dataframes["application"].count())
        # print(len(dataframes["application"].columns))

        df_bureau_balance = aggregate_bureau_balance(dataframes["bureau_balance"])
        df_aggregated = dataframes["bureau"].join(
            df_bureau_balance, "SK_ID_BUREAU", "left"
        )
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

        df_engineered = run_feature_engineering(
            df_feature_store,
            categorical_columns=[
                "CODE_GENDER",
                "FLAG_OWN_CAR",
                "FLAG_OWN_REALTY",
                "NAME_CONTRACT_TYPE",
                "NAME_INCOME_TYPE",
                "NAME_EDUCATION_TYPE",
                "NAME_FAMILY_STATUS",
                "NAME_HOUSING_TYPE",
                "OCCUPATION_TYPE",
                "ORGANIZATION_TYPE",
            ],
        )

        # df_engineered.show(1, vertical=True)

        df_engineered.write.parquet(str(processed_data_path), mode="overwrite")

    model = lgb.LGBMClassifier()

    acc, roc_auc, conf_matrix = train_model(
        processed_data_path, target_col="TARGET", val_size=0.2, model=model
    )

    print(f"Acc: {acc}, Roc auc: {roc_auc}, Matrix: {conf_matrix}")

    spark.stop()


if __name__ == "__main__":
    main()
