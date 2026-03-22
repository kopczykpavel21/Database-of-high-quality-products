"""
Load and filter Excel product data into SQLite database.
Filters: ReturnRate_pct < 1.4 AND ReviewsCount >= 2
"""
import sqlite3
import pandas as pd
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "products.db")
EXCEL_PATH = os.path.join(os.path.dirname(__file__), "..", "mnt", "uploads",
                          "Alza_12categorized_31500_products_15.10.2025.xlsx")

# Configurable thresholds
RETURN_RATE_MAX = 1.4
REVIEWS_MIN = 2


def build_database(excel_path=EXCEL_PATH, db_path=DB_PATH,
                   return_rate_max=RETURN_RATE_MAX, reviews_min=REVIEWS_MIN):
    print(f"Reading Excel file...")
    df = pd.read_excel(excel_path)
    print(f"Total rows: {len(df)}")

    # Rename product name column
    df = df.rename(columns={"c": "Name"})

    # Convert numerics
    for col in ["ReturnRate_pct", "ReviewsCount", "AvgStarRating",
                "StarRatingsCount", "RecommendRate_pct", "Price_CZK",
                "Stars5_Count", "Stars4_Count", "Stars3_Count",
                "Stars2_Count", "Stars1_Count"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Apply filters
    filtered = df[
        (df["ReturnRate_pct"] < return_rate_max) &
        (df["ReviewsCount"] >= reviews_min)
    ].copy()
    print(f"Filtered rows (ReturnRate < {return_rate_max}%, Reviews >= {reviews_min}): {len(filtered)}")

    # Normalise category names
    filtered["Category"] = filtered["Category"].fillna("Uncategorized").str.strip()

    # Select columns for DB
    keep = [
        "Name", "Category", "ProductURL", "Price_CZK",
        "AvgStarRating", "StarRatingsCount", "ReviewsCount",
        "RecommendRate_pct", "ReturnRate_pct",
        "Stars5_Count", "Stars4_Count", "Stars3_Count",
        "Stars2_Count", "Stars1_Count",
        "Description", "SKU"
    ]
    keep = [c for c in keep if c in filtered.columns]
    filtered = filtered[keep].reset_index(drop=True)

    # Write to SQLite
    conn = sqlite3.connect(db_path)
    filtered.to_sql("products", conn, if_exists="replace", index=True,
                    index_label="id")

    # Add source tag
    conn.execute("ALTER TABLE products ADD COLUMN source TEXT DEFAULT 'alza'")

    # Create indexes
    conn.execute("CREATE INDEX IF NOT EXISTS idx_category ON products(Category)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_return_rate ON products(ReturnRate_pct)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_stars ON products(AvgStarRating)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_reviews ON products(ReviewsCount)")
    conn.commit()
    conn.close()
    print(f"Database saved to {db_path}")
    return len(filtered)


if __name__ == "__main__":
    count = build_database()
    print(f"\nDone! {count} products loaded.")
