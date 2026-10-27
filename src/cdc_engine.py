import logging
import polars as pl

def compute_cdc_diff(incoming_records: list[dict], existing_df: pl.DataFrame | None) -> pl.DataFrame:
    logging.info("Executing Change Data Capture (CDC) hash comparison...")

    if not incoming_records:
        logging.warning("No incoming records to compute CDC.")
        return pl.DataFrame()

    # 1. Load incoming raw payloads into Polars LazyFrame
    df_incoming = (
        pl.DataFrame(incoming_records)
        .lazy()
        .select(
            pl.col("id").cast(pl.Int64).alias("item_id"),
            pl.col("title").cast(pl.Utf8),
            pl.col("category").cast(pl.Utf8),
            pl.col("price").cast(pl.Float64),
            pl.col("stock").cast(pl.Int64)
        )
        # Compute tow-level SHA256 signature across all mutable fields
        .with_columns(
            pl.concat_str(
                [pl.col("title"), pl.col("category"), pl.col("price"), pl.col("stock")],
                separator="|"
            ).hash(seed=42).cast(pl.Utf8).alias("row_hash")

        )
        .collect()
    )

    # 2. If warehouse table is currently empty, mark all records as 'INSERT'
    if existing_df is None or existing_df.is_empty():
        logging.info("No prior snapshot found. Tagging all incoming records as 'INSERT'.")
        return df_incoming.with_columns(pl.lit("INSERT").alias("cdc_action"))

    # 3. Perform Left Join against existing warehouse snapshot to identify mutations
    df_cdc = (
        df_incoming.join(
            existing_df.select(["item_id", "row_hash"]),
            on="item_id",
            how="left",
            suffix="_existing"
        )
        .with_columns(
            pl.when(pl.col("row_hash_existing").is_null())
            .then(pl.lit("INSERT"))
            .when(pl.col("row_hash") != pl.col("row_hash_existing"))
            .then(pl.lit("UPDATE"))
            .otherwise(pl.lit("UNCHANGED"))
            .alias("cdc_action")
        )
        .drop("row_hash_existing")
    )

    action_counts = df_cdc.group_by("cdc_action").len()
    logging.info(f"CDC Audit Summary:\n{action_counts}")
    
    return df_cdc