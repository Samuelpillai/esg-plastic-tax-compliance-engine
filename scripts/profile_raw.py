from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]

FILES = {
    "fact_sales": BASE / "data/raw/retail/fact_sales_normalized.csv",
    "dim_products": BASE / "data/raw/retail/dim_products.csv",
    "dim_dates": BASE / "data/raw/retail/dim_dates.csv",
    "amazon": BASE / "data/raw/amazon/amazon_eco-friendly_products.csv",
}

def profile(name: str, path: Path) -> None:
    df = pd.read_csv(path)
    print(f"\n=== {name} ===")
    print("Path:", path)
    print("Rows:", len(df))
    print("Cols:", df.columns.tolist())
    print("Nulls (top 10):")
    print(df.isna().sum().sort_values(ascending=False).head(10))
    print("Head:")
    print(df.head(3))

if __name__ == "__main__":
    for n, p in FILES.items():
        profile(n, p)