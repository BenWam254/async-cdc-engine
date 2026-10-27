import json
import logging
import redis # type: ignore

class RedisQueue:
    def __init__(self, host: str = "localhost", port: int = 6379, queue_name: str = "raw_ingest_queue"):
        self.r = redis.Redis(host=host, port=port, db=0, decode_responses=True)
        self.queue_name = queue_name

    def push_records(self, records: list[dict]) -> None:
        if not records:
            logging.warning("No records provided to push to Redis.")
            return
        logging.info(f"Pushing {len(records)} records to Redis queue '{self.queue_name}'...")
        for record in records:
            self.r.rpush(self.queue_name, json.dumps(record))

    def pop_all_records(self) -> list[dict]:
        logging.info(f"Popping raw items from Redis queue '{self.queue_name}'...")
        records = []
        while True:
            item = self.r.lpop(self.queue_name)
            if not item:
                break
            records.append(json.loads(item))
        logging.info(f"Retrieved {len(records)} payloads from Redis queue.")
        return records