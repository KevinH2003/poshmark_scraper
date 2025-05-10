import requests
from bs4 import BeautifulSoup
import json
import csv
import urllib.parse
import os
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_unique_filename(base_name):
    name, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{name}_{timestamp}{ext}"

def build_url(params):
    base = "https://poshmark.com/category/"
    category = params.get("category", "Men-Shoes")
    colors = params.get("colors", [])
    sizes = params.get("sizes", [])
    brands = params.get("brands", [])
    price_max = params.get("price_max")
    sort_by = params.get("sort_by", "just_in")
    page = params.get("page", 1)

    color_path = ''.join(f"--color-{color}" for color in colors)
    full_path = f"{category}{color_path}"

    query = {
        "sort_by": sort_by,
        "max_id": page,
    }
    for size in sizes:
        query.setdefault("size[]", []).append(size)
    for brand in brands:
        query.setdefault("brand[]", []).append(brand)
    if price_max is not None:
        query["price[]"] = f"-{price_max}"

    query_str = urllib.parse.urlencode(query, doseq=True)
    return f"{base}{full_path}?{query_str}"

def extract_listing_data(listing):
    def safe_text(tag):
        return tag.text.strip() if tag else "N/A"

    title_tag = listing.find("a", class_="tile__title")
    price_tag = listing.find("span", class_="p--t--1 fw--bold")
    link_tag = listing.find("a", class_="tile__covershot")
    size_tag = listing.find("a", class_="tile__details__pipe__size")
    seller_tag = listing.find("a", class_="tile__creator")
    brand_tag = listing.find("a", class_="tile__details__pipe__brand")
    img_tag = listing.find("img")
    like_tag = listing.find("div", class_="social-action-bar__like")
    like_span = like_tag.find("span") if like_tag else None

    image_url = img_tag.get("src") or img_tag.get("data-src") if img_tag else "N/A"
    category_id = listing.get("data-et-prop-category_id", "N/A")

    return {
        "Title": safe_text(title_tag),
        "Price": safe_text(price_tag),
        "Size": safe_text(size_tag).replace("Size: ", ""),
        "Brand": safe_text(brand_tag),
        "Seller": safe_text(seller_tag),
        "URL": f"https://poshmark.com{link_tag['href']}" if link_tag else "N/A",
        "Image": image_url,
        "Likes": like_span.text.strip() if like_span and like_span.text.strip().isdigit() else "0",
        "CategoryID": category_id
    }

def scrape_page(params, page):
    headers = {"User-Agent": "Mozilla/5.0"}
    page_params = params.copy()
    page_params["page"] = page
    url = build_url(page_params)
    print(f"Scraping page {page}: {url}")
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    listings = soup.find_all("div", {"data-et-name": "listing"})
    rows = [extract_listing_data(listing) for listing in listings]
    return rows

def scrape_poshmark(params, output_file="poshmark_listings.csv"):
    all_rows = []
    max_pages = params.get("max_pages")
    max_workers = params.get("max_workers", 5)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape_page, params, page): page for page in range(1, max_pages + 1)}
        for future in as_completed(futures):
            try:
                rows = future.result()
                if not rows:
                    print(f"No listings found on page {futures[future]}")
                all_rows.extend(rows)
            except Exception as e:
                print(f"Error scraping page {futures[future]}: {e}")

    output_file = get_unique_filename(output_file)
    keys = ["Title", "Price", "Size", "Brand", "Seller", "URL", "Image", "Likes", "CategoryID"]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"\nSaved {len(all_rows)} total listings to {output_file}")

def load_params(folder="params", filename="item_params.json"):
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(script_dir, folder, filename)
    with open(full_path, "r") as f:
        return json.load(f)

if __name__ == "__main__":
    PARAMS_FOLDER = "params"
    PARAMS_FILE = "item_params.json"
    params = load_params(PARAMS_FOLDER, PARAMS_FILE)
    scrape_poshmark(params)
