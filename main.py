import asyncio
import logging
import os
from src.scraper import run_async_scrape
from src.queue_worker import RedisQueue
from src.cdc_engine import compute_cdc_diff
from src.loader import get_existing_snapshot, apply_cdc_to_duckdb

async def main():
    logging.info("=== Starting Autonomous Asynchronous CDC Engine ===")

    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))

    # Step 1: Async concurrent extraction
    scraped_data = await run_async_scrape()

    # Step 2: Push & retrieve via Redis Queue
    queue = RedisQueue(host=redis_host, port=redis_port)
    queue.push_records(scraped_data)
    queued_data = queue.pop_all_records()

    # Step 3: Fetch current warehouse state & compute CDC hashes
    existing_snapshot = get_existing_snapshot()
    df_cdc = compute_cdc_diff(queued_data, existing_snapshot)

    # Step 4: Persist CDC diffs to DuckDB
    apply_cdc_to_duckdb(df_cdc)

    logging.info("=== CDC Engine Pipeline Run Completed Successfully ===")

if __name__ == "__main__":
    asyncio.run(main())