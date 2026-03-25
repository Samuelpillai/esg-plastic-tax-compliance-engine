from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parents[1]

# Current UK PPT rate (use param)
PPT_RATE_GBP_PER_TONNE = 223.69

silver = pd.read_parquet(BASE / "data/silver/sales_enriched.parquet")

# Plastic tonnes and tax
silver["plastic_kg_total"] = silver["total_amount"] * 0  # placeholder if you later model qty
# Your retail fact does NOT have quantity; it has total_amount.
# So we treat each row as a sale value and use a proxy units = 1 unless you later model units.
silver["units_proxy"] = 1
silver["plastic_kg_total"] = silver["units_proxy"] * silver["PLASTIC_WEIGHT_KG"]
silver["plastic_tonnes_total"] = silver["plastic_kg_total"] / 1000.0
silver["projected_tax_liability_gbp"] = silver["plastic_tonnes_total"] * PPT_RATE_GBP_PER_TONNE

# Category risk ranking
cat_risk = (
    silver.groupby("category", dropna=False)
    .agg(
        total_tax_gbp=("projected_tax_liability_gbp", "sum"),
        total_plastic_tonnes=("plastic_tonnes_total", "sum"),
        unclassified=("ESG_MATCH_STATUS", lambda s: (s == "UNCLASSIFIED").sum()),
        sales_rows=("sales_id", "count"),
        total_sales_value=("total_amount", "sum"),
    )
    .reset_index()
    .sort_values("total_tax_gbp", ascending=False)
)
cat_risk["risk_ranking"] = range(1, len(cat_risk) + 1)

# Save Gold outputs
gold_dir = BASE / "data/gold"
gold_dir.mkdir(parents=True, exist_ok=True)

silver.to_parquet(gold_dir / "fact_compliance.parquet", index=False)
silver.to_csv(gold_dir / "pbi_export.csv", index=False)
cat_risk.to_csv(gold_dir / "category_risk.csv", index=False)

print("✅ Gold complete:")
print(" -", gold_dir / "fact_compliance.parquet")
print(" -", gold_dir / "pbi_export.csv")
print(" -", gold_dir / "category_risk.csv")