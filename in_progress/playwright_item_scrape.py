import csv
import os
import time
import json
import urllib.parse
from playwright.sync_api import sync_playwright

def get_unique_filename(base_name):
    if not os.path.exists(base_name):
        return base_name
    name, ext = os.path.splitext(base_name)
    counter = 1
    while True:
        new_name = f"{name}_{counter}{ext}"
        if not os.path.exists(new_name):
            return new_name
        counter += 1

def build_url(params, page_number=None):
    base = "https://poshmark.com/category/"
    category = params.get("category", "Men-Shoes")
    colors = params.get("colors", [])
    sizes = params.get("sizes", [])
    brands = params.get("brands", [])
    price_max = params.get("price_max")
    sort_by = params.get("sort_by", "just_in")

    color_path = ''.join(f"--color-{color}" for color in colors)
    full_path = f"{category}{color_path}"

    query = {"sort_by": sort_by}
    for size in sizes:
        query.setdefault("size[]", []).append(size)
    for brand in brands:
        query.setdefault("brand[]", []).append(brand)
    if price_max is not None:
        query["price[]"] = f"-{price_max}"
    if page_number is not None:
        query["max_id"] = str(page_number)

    query_str = urllib.parse.urlencode(query, doseq=True)
    return f"{base}{full_path}?{query_str}"

def scrape_poshmark_with_playwright(params, output_csv="poshmark_results.csv"):
    max_pages = params.get("max_pages", 10)
    collected_urls = set()
    all_rows = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        page = browser.new_page(viewport={"width": 1280, "height": 800})

        for page_number in range(1, max_pages + 1):
            url = build_url(params, page_number=page_number)
            print(f"Loading: {url}")
            page.goto(url, timeout=60000)
            page.wait_for_selector("div[data-et-name='listing']", state="attached", timeout=15000)

            listings = page.query_selector_all("div[data-et-name='listing']")
            print(f"Page {page_number} — Found {len(listings)} listings")
            if not listings:
                break

            new_rows = []
            for listing in listings:
                link = listing.query_selector("a.tile__covershot")
                url_path = link.get_attribute("href") if link else None
                full_url = f"https://poshmark.com{url_path}" if url_path else None
                if not full_url or full_url in collected_urls:
                    continue
                collected_urls.add(full_url)

                title = listing.query_selector("a.tile__title")
                price = listing.query_selector("span.p--t--1.fw--bold")
                size = listing.query_selector("a.tile__details__pipe__size")
                brand = listing.query_selector("a.tile__details__pipe__brand")
                seller = listing.query_selector("a.tile__creator span")
                img = listing.query_selector("img")
                likes = listing.query_selector("div.social-action-bar__like")

                new_rows.append({
                    "Title": title.inner_text().strip() if title else "N/A",
                    "Price": price.inner_text().strip() if price else "N/A",
                    "Size": size.inner_text().strip().replace("Size: ", "") if size else "N/A",
                    "Brand": brand.inner_text().strip() if brand else "N/A",
                    "Seller": seller.inner_text().strip() if seller else "N/A",
                    "URL": full_url,
                    "Image": img.get_attribute("src") or img.get_attribute("data-src") if img else "N/A",
                    "Likes": likes.get_attribute("aria-label") if likes else "N/A"
                })

            if not new_rows:
                print("No new listings found — stopping.")
                break

            all_rows.extend(new_rows)

        output_csv = get_unique_filename(output_csv)
        keys = ["Title", "Price", "Size", "Brand", "Seller", "URL", "Image", "Likes"]
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(all_rows)

        print(f"\nSaved {len(all_rows)} listings to {output_csv}")
        browser.close()

if __name__ == "__main__":
    with open("params.json", "r") as f:
        params = json.load(f)
    scrape_poshmark_with_playwright(params)
