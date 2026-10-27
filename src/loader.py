import logging
from pathlib import Path
import duckdb
import polars as pl

DB_PATH = Path("data/cdc_warehouse.duckdb")

def get_existing_snapshot() -> pl.DataFrame | None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))

    # .fetchall() fully drains the result set, avoiding connection resource locks
    tables = [row[0] for row in conn.execute("SHOW TABLES;").fetchall()]

    if "inventory_snapshot" not in tables:
        conn.close()
        return None

    df_existing = conn.sql("SELECT * FROM inventory_snapshot;").pl()
    conn.close()
    return df_existing

def apply_cdc_to_duckdb(df_cdc: pl.DataFrame) -> None:
    if df_cdc.is_empty():
        logging.info("Empty CDC result. Skipping load.")
        return

    logging.info(f"Writing CDC updates to DuckDB database at {DB_PATH}...")
    conn = duckdb.connect(str(DB_PATH))

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_snapshot (
            item_id BIGINT PRIMARY KEY,
            title VARCHAR,
            category VARCHAR,
            price DOUBLE,
            stock BIGINT,
            row_hash VARCHAR
        );

        CREATE TABLE IF NOT EXISTS cdc_audit_log (
            item_id BIGINT,
            title VARCHAR,
            price DOUBLE,
            stock BIGINT,
            cdc_action VARCHAR,
            logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    conn.register("df_cdc_view", df_cdc)

    conn.execute(
        """
        INSERT INTO cdc_audit_log (item_id, title, price, stock, cdc_action)
        SELECT item_id, title, price, stock, cdc_action
        FROM df_cdc_view
        WHERE cdc_action IN ('INSERT', 'UPDATE');
        """
    )

    conn.execute(
        """
        INSERT INTO inventory_snapshot BY NAME
        SELECT item_id, title, category, price, stock, row_hash
        FROM df_cdc_view
        WHERE cdc_action IN ('INSERT', 'UPDATE')
        ON CONFLICT (item_id) DO UPDATE SET
            title = excluded.title,
            category = excluded.category,
            price = excluded.price,
            stock = excluded.stock,
            row_hash = excluded.row_hash;
        """
    )

    result = conn.execute("SELECT COUNT(*) FROM inventory_snapshot;").fetchone()
    total_items = result[0] if result else 0
    logging.info(f"Load complete. Active inventory snapshot size: {total_items} items.")
    conn.close()