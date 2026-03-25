from pathlib import Path
import pandas as pd
from rapidfuzz import process, fuzz

BASE = Path(__file__).resolve().parents[1]

# ---------- Load local CSVs ----------
fact = pd.read_csv(BASE / "data/raw/retail/fact_sales_normalized.csv")
products = pd.read_csv(BASE / "data/raw/retail/dim_products.csv")
dates = pd.read_csv(BASE / "data/raw/retail/dim_dates.csv")
amazon = pd.read_csv(BASE / "data/raw/amazon/amazon_eco-friendly_products.csv")

# ---------- Helpers ----------
def norm(s) -> str:
    if pd.isna(s):
        return ""
    s = str(s).lower()
    for ch in ["-", "_", "/", "\\", ",", ".", "(", ")", "[", "]", ":", ";", "|"]:
        s = s.replace(ch, " ")
    return " ".join(s.split())

# Ensure expected cols exist
assert {"product_sk", "product_id", "product_name", "category"}.issubset(products.columns)
assert {"sales_date", "product_sk", "total_amount"}.issubset(fact.columns)
assert {"full_date", "year", "month"}.issubset(dates.columns)
assert {"title", "category", "material", "description"}.issubset(amazon.columns)

# ---------- Normalize / prep ----------
products["product_name_norm"] = products["product_name"].map(norm)

amazon["title_norm"] = amazon["title"].map(norm)
amazon_titles = amazon["title_norm"].tolist()

# Parse sales_date timestamp -> date string YYYY-MM-DD for joining dim_dates
fact["sales_date_dt"] = pd.to_datetime(fact["sales_date"], errors="coerce")
fact["sales_day"] = fact["sales_date_dt"].dt.date.astype(str)  # "2024-02-19"

# dim_dates.full_date is already YYYY-MM-DD
dates["full_date"] = pd.to_datetime(dates["full_date"], errors="coerce").dt.date.astype(str)

# ---------- 1) Fuzzy match ONLY unique products (≈210), not 1,000,000 rows ----------
def fuzzy_match_product(pname_norm: str):
    if not pname_norm:
        return pd.Series([None, None, None, 0, "UNCLASSIFIED"])

    hit = process.extractOne(
        pname_norm,
        amazon_titles,
        scorer=fuzz.token_sort_ratio
    )
    if not hit:
        return pd.Series([None, None, None, 0, "UNCLASSIFIED"])

    best_title_norm, score, idx = hit[0], hit[1], hit[2]
    if score < 70:
        return pd.Series([None, None, None, score, "UNCLASSIFIED"])

    row = amazon.iloc[idx]
    return pd.Series([row["title"], row["category"], row["material"], score, "MATCHED"])

# Run fuzzy match on products table only
products[["amazon_title", "amazon_category", "amazon_material", "match_score", "ESG_MATCH_STATUS"]] = (
    products["product_name_norm"].apply(fuzzy_match_product)
)

# ---------- 2) Derive ESG_TAG using title/material/description keywords ----------
# Build a lookup dict for amazon description by title for O(1) access
amazon_desc_by_title = amazon.set_index("title")["description"].to_dict()

def derive_esg_tag(title, material, description) -> str:
    blob = " ".join([norm(title), norm(material), norm(description)])

    eco = any(k in blob for k in [
        "plastic free", "plastic-free", "biodegradable", "compostable",
        "recycled", "recyclable", "bamboo", "stainless steel", "glass",
        "paper", "cardboard", "refillable", "zero waste"
    ])
    plastic = any(k in blob for k in ["plastic", "polyethylene", "polypropylene", "pet", "pvc"])

    if eco and not plastic:
        return "ECO_PREFERRED"
    if plastic and not eco:
        return "PLASTIC_LIKELY"
    if eco and plastic:
        return "MIXED_SIGNALS"
    return "UNKNOWN"

def product_esg_tag(row):
    title = row.get("amazon_title")
    desc = amazon_desc_by_title.get(title, "") if title else ""
    return derive_esg_tag(title, row.get("amazon_material"), desc)

products["ESG_TAG"] = products.apply(product_esg_tag, axis=1)

# ---------- 3) Plastic weight derivation (use product category) ----------
CATEGORY_WEIGHT_KG = {
    "beverages": 0.05,
    "grocery": 0.02,
    "household": 0.04,
    "personal care": 0.03,
}
DEFAULT_WEIGHT = 0.02

def weight_from_category(cat: str) -> float:
    if pd.isna(cat):
        return DEFAULT_WEIGHT
    c = str(cat).lower()
    for k, v in CATEGORY_WEIGHT_KG.items():
        if k in c:
            return v
    return DEFAULT_WEIGHT

def apply_overrides(row) -> float:
    base = weight_from_category(row.get("category"))
    tag = row.get("ESG_TAG", "UNKNOWN")
    mat = norm(row.get("amazon_material"))

    # Override: strong eco packaging signals => 0 plastic
    if tag == "ECO_PREFERRED" and any(x in mat for x in ["plastic free", "glass", "stainless steel", "bamboo", "paper", "cardboard"]):
        return 0.0

    return base

products["PLASTIC_WEIGHT_KG"] = products.apply(apply_overrides, axis=1)
products["IS_TAXABLE"] = (products["PLASTIC_WEIGHT_KG"] > 0).astype(int)

# ---------- 4) Join million-row fact to enriched product dimension (fast merge) ----------
sales_enriched = (
    fact.merge(
        products[[
            "product_sk", "product_id", "product_name", "category", "brand",
            "amazon_title", "amazon_category", "amazon_material",
            "match_score", "ESG_MATCH_STATUS", "ESG_TAG",
            "PLASTIC_WEIGHT_KG", "IS_TAXABLE"
        ]],
        on="product_sk",
        how="left"
    )
    .merge(
        dates[["full_date", "year", "month", "quarter", "weekday"]],
        left_on="sales_day",
        right_on="full_date",
        how="left"
    )
)

# ---------- Save Silver ----------
out_dir = BASE / "data/silver"
out_dir.mkdir(parents=True, exist_ok=True)

# Parquet is much faster + smaller
sales_enriched.to_parquet(out_dir / "sales_enriched.parquet", index=False)

print("✅ Silver complete:", out_dir / "sales_enriched.parquet")
print("\nSample enriched rows:")
print(sales_enriched[[
    "sales_id", "product_name", "category",
    "amazon_title", "match_score", "ESG_MATCH_STATUS",
    "ESG_TAG", "PLASTIC_WEIGHT_KG", "IS_TAXABLE",
    "year", "month"
]].head(10))