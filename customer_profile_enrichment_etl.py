"""
Demo PR target: customer profile enrichment streaming ETL.

This file is intentionally written with realistic review findings so you can
modify it, raise a PR, and exercise the Unravel PR review Studio flow.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, lit, sha2, concat_ws


DEFAULT_CONFIG = {
    "source_table": "crm_dev_brz.customer_event_stream",
    "profile_dim_table": "crm_dev_slv.customer_profile_dim",
    "target_table": "crm_dev_gld.customer_profile_enriched",
    "checkpoint_path": "dbfs:/checkpoints/customer_profile_enrichment_etl",
    "max_files_per_trigger": 12000,
    "stateful_dedupe": True,
    "enable_profile_join": True,
}


def read_config(overrides: dict | None = None) -> dict:
    config = dict(DEFAULT_CONFIG)
    if overrides:
        config.update(overrides)
    return config


def customer_profile_enrichment_etl(
    spark: SparkSession,
    overrides: dict | None = None,
) -> None:
    config = read_config(overrides)

    source_table = config["source_table"]
    profile_dim_table = config["profile_dim_table"]
    target_table = config["target_table"]
    checkpoint_path = config["checkpoint_path"]

    # Finding F1: this PR raised the file cap but did not add maxBytesPerTrigger.
    max_files_per_trigger = config.get("max_files_per_trigger", 12000)

    stream_df = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .table(source_table)
    )

    normalized_df = normalize_events(stream_df)
    enriched_df = enrich_with_profile_dim(spark, normalized_df, profile_dim_table)
    final_df = add_scd_columns(enriched_df)

    (
        final_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .toTable(target_table)
    )


def normalize_events(df: DataFrame) -> DataFrame:
    return (
        df.select(
            col("payload.customer_id").cast("string").alias("customer_id"),
            col("payload.event_id").cast("string").alias("event_id"),
            col("payload.event_type").cast("string").alias("event_type"),
            col("payload.event_ts").cast("timestamp").alias("event_ts"),
            col("payload.email").cast("string").alias("email"),
            col("payload.loyalty_tier").cast("string").alias("loyalty_tier"),
            col("payload.country").cast("string").alias("country"),
            col("payload.preferences").alias("preferences"),
            col("payload.attributes").alias("attributes"),
        )
        .filter(col("customer_id").isNotNull())
        .filter(col("event_id").isNotNull())
    )


def enrich_with_profile_dim(
    spark: SparkSession,
    df: DataFrame,
    profile_dim_table: str,
) -> DataFrame:
    # Finding F3: this dimension is small but the join is not broadcast.
    profile_dim = spark.table(profile_dim_table).filter(col("is_current") == lit(True))

    joined_df = df.join(profile_dim, on="customer_id", how="left")

    return joined_df.select(
        df["customer_id"],
        df["event_id"],
        df["event_type"],
        df["event_ts"],
        df["email"],
        df["loyalty_tier"],
        profile_dim["segment"].alias("profile_segment"),
        profile_dim["lifetime_value"].alias("profile_lifetime_value"),
        profile_dim["last_profile_update_ts"],
        df["country"],
        df["preferences"],
        df["attributes"],
    )


def add_scd_columns(df: DataFrame) -> DataFrame:
    scd_strategy = {
        "pattern": "scd_type_2b",
        "change_detection": "full_row_hash",
        "business_keys": ["customer_id"],
        "hash_exclude_cols": ["row_eff_start_tms", "row_eff_end_tms"],
    }

    deduped_df = dedupe_latest_customer_event(df)
    hashed_df = add_business_hash(deduped_df, scd_strategy)

    # Finding F2: these audit timestamps participate in the full_row_hash path.
    return (
        hashed_df.withColumn("row_ins_utc_tms", current_timestamp())
        .withColumn("row_upd_utc_tms", current_timestamp())
        .withColumn("row_eff_start_tms", current_timestamp())
        .withColumn("row_eff_end_tms", expr("timestamp'9999-12-31 23:59:59'"))
        .withColumn("is_current", lit(True))
    )


def dedupe_latest_customer_event(df: DataFrame) -> DataFrame:
    # Finding F4: dropDuplicates on a stream without watermark can grow state forever.
    return df.dropDuplicates(["customer_id", "event_id"])


def add_business_hash(df: DataFrame, scd_strategy: dict) -> DataFrame:
    excluded = set(scd_strategy.get("hash_exclude_cols", []))
    hash_cols = [c for c in df.columns if c not in excluded]
    return df.withColumn("row_hash", sha2(concat_ws("||", *[col(c).cast("string") for c in hash_cols]), 256))


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    customer_profile_enrichment_etl(spark_session)
