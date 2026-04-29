from pyspark.sql import DataFrame
from pyspark.sql import functions as sf


def aggregate_bureau_balance(df: DataFrame) -> DataFrame:
    aggregated_df = df.groupBy("SK_ID_BUREAU").agg(
        sf.count(df["MONTHS_BALANCE"]).alias("bb_months_count"),
        sf.sum(
            sf.when(df["STATUS"].isin(["1", "2", "3", "4", "5"]), 1).otherwise(0),
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


def aggregate_previous_application(df: DataFrame) -> DataFrame:
    aggregated_df = df.groupBy("SK_ID_CURR").agg(
        sf.count(df["SK_ID_PREV"]).alias("prev_app_total_count"),
        sf.sum(sf.when(df["NAME_CONTRACT_STATUS"] == "Approved", 1).otherwise(0)).alias(
            "prev_app_approved_count",
        ),
        sf.sum(sf.when(df["NAME_CONTRACT_STATUS"] == "Refused", 1).otherwise(0)).alias(
            "prev_app_refused_count",
        ),
        sf.sum(sf.when(df["NAME_CONTRACT_STATUS"] == "Canceled", 1).otherwise(0)).alias(
            "prev_app_canceled_count",
        ),
        sf.mean(df["AMT_APPLICATION"]).alias("prev_app_mean_application_amt"),
        sf.mean(df["AMT_CREDIT"]).alias("prev_app_mean_credit_amt"),
        sf.mean(df["CNT_PAYMENT"]).alias("prev_app_mean_installments"),
        sf.max(df["DAYS_DECISION"]).alias("prev_app_last_decision_days"),
    )

    return aggregated_df


def aggregate_installments(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "payment_delay_days",
        df["DAYS_ENTRY_PAYMENT"] - df["DAYS_INSTALMENT"],
    )
    df = df.withColumn("payment_shortfall", df["AMT_INSTALMENT"] - df["AMT_PAYMENT"])

    aggregated_df = df.groupBy("SK_ID_CURR").agg(
        sf.mean("payment_delay_days").alias("inst_mean_delay_days"),
        sf.max("payment_delay_days").alias("inst_max_delay_days"),
        sf.mean("payment_shortfall").alias("inst_mean_shortfall"),
        sf.mean(df["AMT_INSTALMENT"]).alias("inst_mean_instalment_amt"),
        sf.count(df["SK_ID_PREV"]).alias("inst_total_count"),
    )

    return aggregated_df


def aggregate_pos_cash_balance(df: DataFrame) -> DataFrame:
    aggregated_df = df.groupBy("SK_ID_CURR").agg(
        sf.mean(df["CNT_INSTALMENT"]).alias("pos_mean_instalment"),
        sf.sum(df["CNT_INSTALMENT_FUTURE"]).alias("pos_sum_future_instalments"),
        sf.sum(sf.when(df["NAME_CONTRACT_STATUS"] == "Active", 1).otherwise(0)).alias(
            "pos_active_count",
        ),
        sf.sum(
            sf.when(df["NAME_CONTRACT_STATUS"] == "Completed", 1).otherwise(0),
        ).alias("pos_completed_count"),
        sf.mean(df["SK_DPD"]).alias("pos_mean_dpd"),
        sf.max(df["SK_DPD"]).alias("pos_max_dpd"),
        sf.mean(df["SK_DPD_DEF"]).alias("pos_mean_dpd_def"),
        sf.count(df["SK_ID_PREV"]).alias("pos_total_count"),
    )

    return aggregated_df


def aggregate_credit_card_balance(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "credit_utilization",
        sf.when(
            df["AMT_CREDIT_LIMIT_ACTUAL"] > 0,
            df["AMT_BALANCE"] / df["AMT_CREDIT_LIMIT_ACTUAL"],
        ).otherwise(None),
    )

    aggregated_df = df.groupBy("SK_ID_CURR").agg(
        sf.mean("SK_DPD").alias("cc_mean_dpd"),
        sf.max("SK_DPD").alias("cc_max_dpd"),
        sf.mean("SK_DPD_DEF").alias("cc_mean_dpd_def"),
        sf.mean("credit_utilization").alias("cc_mean_utilization"),
        sf.max("credit_utilization").alias("cc_max_utilization"),
        sf.mean("AMT_PAYMENT_CURRENT").alias("cc_mean_payment"),
        sf.mean("AMT_INST_MIN_REGULARITY").alias("cc_mean_min_payment"),
        sf.mean("AMT_DRAWINGS_CURRENT").alias("cc_mean_drawings_amt"),
        sf.mean("CNT_DRAWINGS_CURRENT").alias("cc_mean_drawings_count"),
        sf.sum(sf.when(df["NAME_CONTRACT_STATUS"] == "Active", 1).otherwise(0)).alias(
            "cc_active_count",
        ),
        sf.count("SK_ID_PREV").alias("cc_total_months"),
    )

    return aggregated_df
