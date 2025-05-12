import os
import sqlite3
import pandas as pd
import argparse
import re
from datetime import datetime
from tqdm import tqdm

def find_latest_profiles_csv(prefix="seller_profiles_", extension=".csv"):
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
        print("No timestamped seller profiles CSV found.")
        return None

    timestamped.sort(reverse=True)
    return timestamped[0][1]

def create_database_from_folder(item_folder, profiles_csv=None, db_path="poshmark_listings.db", include_media=False):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create sellers table if profiles provided
    if profiles_csv and os.path.exists(profiles_csv):
        seller_df = pd.read_csv(profiles_csv)
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
        cursor.execute("DELETE FROM sellers")
        seller_df.to_sql("sellers", conn, if_exists="append", index=False)
        print(f"✅ Seller profiles loaded from {profiles_csv}")

    # Define listing schema
    listing_columns = [
        ("Seller", "TEXT"),
        ("Title", "TEXT"),
        ("Price", "TEXT"),
        ("Size", "TEXT"),
        ("Brand", "TEXT"),
        ("Likes", "INTEGER"),
        ("CategoryName", "TEXT")
    ]
    if include_media:
        listing_columns += [("Image", "TEXT"), ("ItemURL", "TEXT")]

    # Create listings table
    cursor.execute("DROP TABLE IF EXISTS listings")
    column_defs = ",\n".join([f"{col} {dtype}" for col, dtype in listing_columns])
    cursor.execute(f"""
        CREATE TABLE listings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {column_defs}
        )
    """)

    total_inserted = 0
    files = [f for f in os.listdir(item_folder) if f.endswith(".csv") and f.startswith("items_")]

    for fname in tqdm(files, desc="Importing listings", unit="file"):
        path = os.path.join(item_folder, fname)
        try:
            df = pd.read_csv(path)
            if "Seller" not in df.columns:
                tqdm.write(f"❌ Skipping {fname} (no Seller column)")
                continue

            drop_cols = ["CategoryID"]
            if not include_media:
                drop_cols += ["Image", "ItemURL"]

            for col in drop_cols:
                if col in df.columns:
                    df.drop(columns=col, inplace=True)

            df.to_sql("listings", conn, if_exists="append", index=False)
            total_inserted += len(df)
        except Exception as e:
            tqdm.write(f"❌ Error with {fname}: {e}")

    conn.commit()
    conn.close()
    print(f"\n✅ Total inserted: {total_inserted:,} listings")

def main():
    parser = argparse.ArgumentParser(description="Create a SQLite database from seller item CSVs in a folder.")
    parser.add_argument("--folder", type=str, default="seller_items", help="Folder containing item CSVs")
    parser.add_argument("--profiles", type=str, default="", help="Path to the seller profiles CSV file")
    parser.add_argument("--output", type=str, default="poshmark_listings.db", help="Output SQLite database filename")
    parser.add_argument("--include-media", action="store_true", help="Include Image and ItemURL fields")
    args = parser.parse_args()

    profiles_csv = args.profiles if args.profiles else find_latest_profiles_csv()
    create_database_from_folder(args.folder, profiles_csv=profiles_csv, db_path=args.output, include_media=args.include_media)

if __name__ == "__main__":
    main()
