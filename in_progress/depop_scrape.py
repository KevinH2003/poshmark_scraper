import requests
import csv
import os
import time
import random
import json
from datetime import datetime
from tqdm import tqdm


def get_unique_filename(base_name):
    name, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{name}_{timestamp}{ext}"


def load_params(path="params/depop_params.json"):
    with open(path, "r") as f:
        return json.load(f)


def build_initial_url(params):
    base = "https://webapi.depop.com/api/v3/search/products/"
    return base, {
        "items_per_page": params.get("items_per_page", 24),
        "sort": params.get("sort_by", "priceAscending"),
        "country": params.get("country", "us"),
        "currency": params.get("currency", "USD"),
        "gender": params.get("gender", "male"),
        "groups": params.get("group", "footwear"),
        "product_types": params.get("product_type", "trainers"),
    }


def extract_listing_data(product):
    return {
        "ID": product.get("id"),
        "Title": product.get("title"),
        "Price": product.get("price", {}).get("amount"),
        "Currency": product.get("price", {}).get("currency"),
        "Brand": product.get("brand", {}).get("name"),
        "Size": product.get("size"),
        "Seller": product.get("seller", {}).get("username"),
        "URL": f"https://www.depop.com/products/{product.get('id')}" if product.get("id") else "",
        "Image": product.get("picture", {}).get("url")
    }


def scrape_depop(params):
    headers = {"User-Agent": "Mozilla/5.0"}
    base_url, query = build_initial_url(params)
    cursor = None
    max_pages = params.get("max_pages", 10)
    delay_range = params.get("delay_range", [0.2, 1.0])
    output_file = get_unique_filename(params.get("output_file", "depop_listings.csv"))

    all_rows = []
    page = 1

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "ID", "Title", "Price", "Currency", "Size", "Brand", "Seller", "URL", "Image"
        ])
        writer.writeheader()

        while page <= max_pages:
            if cursor:
                query["cursor"] = cursor
            else:
                query.pop("cursor", None)

            print(f"📦 Scraping page {page}...")
            try:
                res = requests.get(base_url, params=query, headers=headers)
                data = res.json()
                products = data.get("products", [])

                if not products:
                    print("❌ No products found, stopping.")
                    break

                rows = [extract_listing_data(p) for p in products]
                writer.writerows(rows)
                all_rows.extend(rows)

                cursor = data.get("cursor")
                if not cursor:
                    print("✅ No more pages.")
                    break

                page += 1
                time.sleep(random.uniform(*delay_range))
            except Exception as e:
                print(f"❌ Error on page {page}: {e}")
                break

    print(f"\n✅ Saved {len(all_rows)} listings to {output_file}")


if __name__ == "__main__":
    params = load_params()
    scrape_depop(params)
