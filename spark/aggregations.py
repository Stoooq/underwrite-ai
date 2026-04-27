from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def aggregate_bureau(df: DataFrame) -> DataFrame:
    aggregated_df = df.groupBy("SK_ID_CURR").agg(
        sf.sum(df["AMT_CREDIT_SUM"]).alias("bureau_sum_credit"),
        sf.sum(df["AMT_CREDIT_SUM_DEBT"]).alias("bureau_sum_debt"),
        sf.max(df["AMT_CREDIT_MAX_OVERDUE"]).alias("bureau_max_overdue_amt"),
        sf.mean(df["AMT_CREDIT_MAX_OVERDUE"]).alias("bureau_mean_overdue_amt"),
        sf.max(df["CREDIT_DAY_OVERDUE"]).alias("bureau_max_overdue_days"),
        sf.sum(df["CNT_CREDIT_PROLONG"]).alias("bureau_sum_prolongations"),
        sf.count(df["SK_ID_BUREAU"]).alias("bureau_total_count"),
        sf.sum(sf.when(df["CREDIT_ACTIVE"] == "Active", 1).otherwise(0)).alias("bureau_active_count"),
        sf.sum(sf.when(df["CREDIT_ACTIVE"] == "Bad debt", 1).otherwise(0)).alias("bureau_bad_debt_count"),
        sf.sum(sf.when(df["CREDIT_ACTIVE"] == "Closed", 1).otherwise(0)).alias("bureau_closed_count"),
    )

    aggregated_df.show(vertical=True)

    return aggregated_df
