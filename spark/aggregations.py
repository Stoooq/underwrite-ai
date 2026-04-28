from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def aggregate_bureau_balance(df: DataFrame) -> DataFrame:
    aggregated_df = df.groupBy("SK_ID_BUREAU").agg(
        sf.count(df["MONTHS_BALANCE"]).alias("bb_months_count"),
        sf.sum(
            sf.when(df["STATUS"].isin(["1", "2", "3", "4", "5"]), 1).otherwise(0)
        ).alias(
            "bb_dpd_count",
        ),
        sf.sum(sf.when(df["STATUS"].isin(["3", "4", "5"]), 1).otherwise(0)).alias(
            "bb_severe_dpd_count",
        ),
        sf.max(
            sf.when(
                df["STATUS"].isin(["1", "2", "3", "4", "5"]),
                df["STATUS"].cast("int"),
            ).otherwise(None),
        ).alias("bb_max_dpd_status"),
    )

    return aggregated_df


def aggregate_bureau(df: DataFrame) -> DataFrame:
    aggregated_df = df.groupBy("SK_ID_CURR").agg(
        sf.sum(df["AMT_CREDIT_SUM"]).alias("bureau_sum_credit"),
        sf.sum(df["AMT_CREDIT_SUM_DEBT"]).alias("bureau_sum_debt"),
        sf.max(df["AMT_CREDIT_MAX_OVERDUE"]).alias("bureau_max_overdue_amt"),
        sf.mean(df["AMT_CREDIT_MAX_OVERDUE"]).alias("bureau_mean_overdue_amt"),
        sf.max(df["CREDIT_DAY_OVERDUE"]).alias("bureau_max_overdue_days"),
        sf.sum(df["CNT_CREDIT_PROLONG"]).alias("bureau_sum_prolongations"),
        sf.count(df["SK_ID_BUREAU"]).alias("bureau_total_count"),
        sf.sum(sf.when(df["CREDIT_ACTIVE"] == "Active", 1).otherwise(0)).alias(
            "bureau_active_count",
        ),
        sf.sum(sf.when(df["CREDIT_ACTIVE"] == "Bad debt", 1).otherwise(0)).alias(
            "bureau_bad_debt_count",
        ),
        sf.sum(sf.when(df["CREDIT_ACTIVE"] == "Closed", 1).otherwise(0)).alias(
            "bureau_closed_count",
        ),
        sf.sum(df["bb_months_count"]).alias("bureau_bb_total_months"),
        sf.sum(df["bb_dpd_count"]).alias("bureau_bb_dpd_count"),
        sf.sum(df["bb_severe_dpd_count"]).alias("bureau_bb_severe_dpd_count"),
        sf.max(df["bb_max_dpd_status"]).alias("bureau_bb_max_dpd"),
    )

    return aggregated_df
