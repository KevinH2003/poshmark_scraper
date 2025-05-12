import os
import pandas as pd
import re

def add_seller_column_to_csvs(folder="seller_items"):
    total = 0
    for fname in os.listdir(folder):
        if fname.endswith(".csv") and fname.startswith("items_"):
            match = re.match(r"items_(.+?)_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.csv", fname)
            if not match:
                print(f"❌ Skipping unrecognized file format: {fname}")
                continue

            seller = match.group(1)
            path = os.path.join(folder, fname)

            try:
                df = pd.read_csv(path)
                if "Seller" not in df.columns:
                    df["Seller"] = seller
                    df.to_csv(path, index=False)
                    total += 1
                    # print(f"✅ Updated: {fname}")
                else:
                    print(f"⚠️  Skipped (Seller column already exists): {fname}")
            except Exception as e:
                print(f"❌ Error processing {fname}: {e}")

    print(f"Saved {total} total files")

if __name__ == "__main__":
    add_seller_column_to_csvs()
