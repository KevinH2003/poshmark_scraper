import os
import sqlite3
import pandas as pd
import argparse
import re
from datetime import datetime

def load_csvs_to_dataframe(folder):
    all_rows = []
    for fname in os.listdir(folder):
        if fname.endswith(".csv") and fname.startswith("items_"):
            seller = fname.split("items_")[1].rsplit(".", 1)[0]
            path = os.path.join(folder, fname)
            df = pd.read_csv(path)
            df["Seller"] = seller
            all_rows.append(df)
    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()

def create_database(profiles_csv, db_path="poshmark_listings.db"):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create sellers table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS sellers (
        Seller TEXT PRIMARY KEY,
        Listings TEXT,
        Followers TEXT,
        Following TEXT,
        ItemCount INTEGER,
        URL TEXT,
        ItemCSV TEXT
    )
    """)

    # Insert seller profile data
    seller_df = pd.read_csv(profiles_csv)
    seller_df.to_sql("sellers", conn, if_exists="replace", index=False)

    # Create listings table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS listings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        Seller TEXT,
        Title TEXT,
        Price TEXT,
        Size TEXT,
        Brand TEXT,
        Image TEXT,
        Likes INTEGER,
        ItemURL TEXT
    )
    """)

    # Load and insert item CSVs from the seller profile table
    all_rows = []
    for _, row in seller_df.iterrows():
        item_path = row["ItemCSV"]
        if os.path.exists(item_path):
            df = pd.read_csv(item_path)
            df["Seller"] = row["Seller"]
            all_rows.append(df)

    if all_rows:
        all_items_df = pd.concat(all_rows, ignore_index=True)
        all_items_df.to_sql("listings", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()

def find_latest_csv(prefix="seller_profiles_", extension=".csv"):
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

def main():
    parser = argparse.ArgumentParser(description="Create a SQLite database from seller profiles and item listings.")
    parser.add_argument("--profiles", type=str, default="", help="Path to the seller profiles CSV file.")
    parser.add_argument("--output", type=str, default="poshmark_listings.db", help="Output SQLite database filename.")
    args = parser.parse_args()

    profiles = args.profiles
    if profiles == "" or not profiles:
        profiles = find_latest_csv("seller_profiles_")

    create_database(profiles, db_path=args.output)
    print(f"✅ Database '{args.output}' created from {profiles}'")

if __name__ == "__main__":
    main()
