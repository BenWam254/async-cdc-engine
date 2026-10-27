import asyncio
import logging
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

TARGET_URLS = [
    "https://dummyjson.com/products?limit=10&select=title,price,category,stock",
    "https://dummyjson.com/products?limit=10&skip=10&select=title,price,category,stock",
    "https://dummyjson.com/products?limit=10&skip=20&select=title,price,category,stock",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

def get_fallback_mock_data() -> list[dict]:
    logging.info("Generating local fallback dataset for CDC engine execution...")
    return [
        {"id": 1, "title": "Wireless Mechanical Keyboard", "price": 89.99, "category": "electronics", "stock": 45},
        {"id": 2, "title": "Ergonomic Mesh Chair", "price": 199.50, "category": "furniture", "stock": 12},
        {"id": 3, "title": "4K USB-C Monitor", "price": 320.00, "category": "electronics", "stock": 8},
        {"id": 4, "title": "Noise-Canceling Earbuds", "price": 59.99, "category": "electronics", "stock": 60},
        {"id": 5, "title": "Desk Mat Ultra", "price": 25.00, "category": "accessories", "stock": 100},
    ]

async def fetch_endpoint(client: httpx.AsyncClient, url: str) -> list[dict]:
    logging.info(f"Async fetching: {url}")
    try:
        response = await client.get(url, timeout=5.0)
        response.raise_for_status()
        data = response.json()
        return data.get("products", [])
    except Exception as err:
        logging.error(f"Failed to fetch {url}: {err}")
        return []

async def run_async_scrape() -> list[dict]:
    logging.info("Starting concurrent async scraping...")
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        tasks = [fetch_endpoint(client, url) for url in TARGET_URLS]
        results = await asyncio.gather(*tasks)

    flattened_records = [item for sublist in results for item in sublist]

    if not flattened_records:
        logging.warning("Async scrape yielded 0 remote records. Falling back to local payload.")
        return get_fallback_mock_data()

    logging.info(f"Extracted {len(flattened_records)} total records concurrently.")
    return flattened_records