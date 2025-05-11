import csv
import os
import time
import json
import requests
import re
import random
from datetime import datetime
import pandas as pd
from bs4 import BeautifulSoup
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import gc

def find_latest_csv(prefix="poshmark_listings_", extension=".csv"):
    files = [f for f in os.listdir(".") if f.startswith(prefix) and f.endswith(extension)]
    timestamped = []
    for f in files:
        match = re.search(r"_(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})", f)
        if match:
            try:
                ts = datetime.strptime(match.group(1), "%Y-%m-%d_%H-%M-%S")
                timestamped.append((ts, f))
            except ValueError:
                continue
    if not timestamped:
        print("No timestamped CSVs found.")
        return None
    timestamped.sort(reverse=True)
    return timestamped[0][1]

def get_unique_filename(base_name):
    name, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return f"{name}_{timestamp}{ext}"

def extract_stats_from_profile(html):
    soup = BeautifulSoup(html, "html.parser")
    stats = {"Listings": "N/A", "Followers": "N/A", "Following": "N/A"}
    def get_stat(selector):
        tag = soup.select_one(selector)
        return tag.text.strip().replace(",", "") if tag else "N/A"
    stats["Listings"] = get_stat("[data-test='closet_listings_count']")
    stats["Followers"] = get_stat("[data-test='closet_followers_count']")
    stats["Following"] = get_stat("[data-test='closet_following_count']")
    soup.decompose()
    del soup
    return stats

def extract_listing_data(listing):
    def safe_text(tag):
        return tag.text.strip() if tag else "N/A"
    title_tag = listing.find("a", class_="tile__title")
    price_tag = listing.find("span", class_="p--t--1 fw--bold")
    link_tag = listing.find("a", class_="tile__covershot")
    size_tag = listing.find("a", class_="tile__details__pipe__size")
    brand_tag = listing.find("a", class_="tile__details__pipe__brand")
    img_tag = listing.find("img")
    like_tag = listing.find("div", class_="social-action-bar__like")
    like_span = like_tag.find("span") if like_tag else None
    image_url = img_tag.get("src") or img_tag.get("data-src") if img_tag else "N/A"
    category_tag = listing.find(attrs={"data-et-prop-category_id": True})
    category_id = category_tag["data-et-prop-category_id"] if category_tag else "N/A"
    category_name = "N/A"
    if size_tag and size_tag.has_attr("href"):
        match = re.search(r"/category/([^/?#]+)", size_tag["href"])
        if match:
            category_name = match.group(1).replace("-", " > ").replace("_", " ")
    return {
        "Title": safe_text(title_tag),
        "Price": safe_text(price_tag),
        "Size": safe_text(size_tag).replace("Size: ", ""),
        "Brand": safe_text(brand_tag),
        "Image": image_url,
        "Likes": like_span.text.strip() if like_span and like_span.text.strip().isdigit() else "0",
        "ItemURL": f"https://poshmark.com{link_tag['href']}" if link_tag else "N/A",
        "CategoryID": category_id,
        "CategoryName": category_name,
    }

def build_seller_url(seller, closet_params=None, page=None):
    base_url = f"https://poshmark.com/closet/{seller}"
    if closet_params:
        query = closet_params.copy()
        if page is not None:
            query["max_id"] = page
        query_str = urllib.parse.urlencode(query, doseq=True)
        return f"{base_url}?{query_str}"
    elif page is not None:
        return f"{base_url}?max_id={page}"
    return base_url

def scrape_all_seller_items(seller, headers, closet_params, max_pages, delay_range, item_output_folder):
    stats = {"Listings": "N/A", "Followers": "N/A", "Following": "N/A"}
    item_filename = get_unique_filename(os.path.join(item_output_folder, f"items_{seller}.csv"))
    fieldnames = ["Title", "Price", "Size", "Brand", "Image", "Likes", "ItemURL", "CategoryID", "CategoryName"]
    with open(item_filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for page in range(1, max_pages + 1 if max_pages else 999):
            url = build_seller_url(seller, closet_params, page=page)
            try:
                response = requests.get(url, headers=headers, timeout=15)
                response.raise_for_status()
                if page == 1:
                    stats = extract_stats_from_profile(response.text)
                soup = BeautifulSoup(response.text, "html.parser")
                listings = soup.find_all("div", {"data-et-name": "listing"})
                if not listings:
                    break
                items = [extract_listing_data(l) for l in listings]
                writer.writerows(items)
                soup.decompose()
                del soup
                if len(listings) < 48:
                    break
                time.sleep(random.uniform(*delay_range))
                gc.collect()
            except Exception as e:
                print(f"❌ Error on page {page} for seller {seller}: {e}")
                break
    return stats, item_filename

def process_single_seller(i, seller, headers, closet_params, max_pages, delay_range, item_output_folder):
    stats, item_filename = scrape_all_seller_items(seller, headers, closet_params, max_pages, delay_range, item_output_folder)
    return {
        "Seller": seller,
        "Listings": stats.get("Listings", "N/A"),
        "Followers": stats.get("Followers", "N/A"),
        "Following": stats.get("Following", "N/A"),
        "ItemCount": sum(1 for _ in open(item_filename)) - 1,
        "URL": build_seller_url(seller),
        "ItemCSV": item_filename,
    }

def scrape_seller_profiles(params):
    input_file = params.get("input_file")
    summary_output_file = params.get("output_file") or "seller_profiles.csv"
    item_output_folder = params.get("item_output_folder") or "seller_items"
    delay_range = params.get("delay_range", [0.1, 0.5])
    append_sellers = params.get("append_sellers", 0)
    closet_params = params.get("closet_params", {})
    seller_range = params.get("seller_range", [1, None])
    max_pages = params.get("max_pages")
    max_workers = params.get("max_workers", 5)

    if not input_file:
        input_file = find_latest_csv()

    if not os.path.exists(summary_output_file) or not append_sellers:
        if not append_sellers:
            summary_output_file = get_unique_filename(summary_output_file)
        write_mode = "w"
        print(f"Output file doesn't exist, creating output file {summary_output_file}")
    else:
        write_mode = "a"
        print(f"Appending to {summary_output_file}")

    os.makedirs(item_output_folder, exist_ok=True)

    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        return

    df = pd.read_csv(input_file)
    sellers = sorted(set(df["Seller"].dropna()))

    start_index = max(0, seller_range[0] - 1)
    end_index = seller_range[1] if seller_range[1] is not None else len(sellers)
    selected_sellers = sellers[start_index:end_index]

    headers = {"User-Agent": "Mozilla/5.0"}
    keys = ["Seller", "Listings", "Followers", "Following", "ItemCount", "URL", "ItemCSV"]
    with open(summary_output_file, write_mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        if write_mode == "w":
            writer.writeheader()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_single_seller, i + start_index + 1, seller, headers, closet_params, max_pages, delay_range, item_output_folder) for i, seller in enumerate(selected_sellers)]
            for future in tqdm(as_completed(futures), total=len(futures), desc="Scraping Sellers", unit="seller"):
                try:
                    row = future.result()
                    writer.writerow(row)
                except Exception as e:
                    print(f"❌ Error in thread: {e}")
    print(f"\n✅ Saved summary to {summary_output_file} and item files to {item_output_folder}/")

def load_params(folder="params", filename="item_params.json"):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(script_dir, folder, filename)
    with open(full_path, "r") as f:
        return json.load(f)

if __name__ == "__main__":
    PARAMS_FOLDER = "params"
    PARAMS_FILE = "seller_params.json"
    params = load_params(PARAMS_FOLDER, PARAMS_FILE)
    scrape_seller_profiles(params)
