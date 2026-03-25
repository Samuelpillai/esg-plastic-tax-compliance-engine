from pathlib import Path
import pandas as pd

# --------------------------------------------------
# Project base directory
# --------------------------------------------------
BASE_DIR = Path(__file__).resolve().parents[1]
REF_DIR = BASE_DIR / "data" / "reference"
REF_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# 1) Category → Plastic Weight Reference Table
# --------------------------------------------------
category_plastic_weight = pd.DataFrame([
    {
        "category": "Beverages",
        "assumed_plastic_weight_kg": 0.05,
        "rationale": "Typical PET bottle or plastic cap packaging"
    },
    {
        "category": "Grocery",
        "assumed_plastic_weight_kg": 0.02,
        "rationale": "Light plastic wrapping, pouches, or trays"
    },
    {
        "category": "Household",
        "assumed_plastic_weight_kg": 0.04,
        "rationale": "Heavier detergent or cleaning product containers"
    },
    {
        "category": "Personal Care",
        "assumed_plastic_weight_kg": 0.03,
        "rationale": "Plastic tubes, pump bottles, or cosmetic containers"
    },
    {
        "category": "Other",
        "assumed_plastic_weight_kg": 0.02,
        "rationale": "Default fallback for uncategorized products"
    },
])

category_weight_path = REF_DIR / "category_plastic_weight.csv"
category_plastic_weight.to_csv(category_weight_path, index=False)

# --------------------------------------------------
# 2) ESG Keyword Taxonomy Reference Table
# --------------------------------------------------
esg_keywords = pd.DataFrame([
    # --- Eco-positive / plastic-free ---
    {"keyword": "plastic free", "esg_signal": "ECO_PREFERRED", "notes": "Explicit plastic-free claim"},
    {"keyword": "plastic-free", "esg_signal": "ECO_PREFERRED", "notes": "Explicit plastic-free claim"},
    {"keyword": "biodegradable", "esg_signal": "ECO_PREFERRED", "notes": "Biodegradable materials"},
    {"keyword": "compostable", "esg_signal": "ECO_PREFERRED", "notes": "Compostable packaging"},
    {"keyword": "recyclable", "esg_signal": "ECO_PREFERRED", "notes": "Recyclable materials"},
    {"keyword": "recycled", "esg_signal": "ECO_PREFERRED", "notes": "Contains recycled material"},
    {"keyword": "bamboo", "esg_signal": "ECO_PREFERRED", "notes": "Natural fiber alternative"},
    {"keyword": "glass", "esg_signal": "ECO_PREFERRED", "notes": "Non-plastic packaging"},
    {"keyword": "stainless steel", "esg_signal": "ECO_PREFERRED", "notes": "Reusable metal packaging"},
    {"keyword": "paper", "esg_signal": "ECO_PREFERRED", "notes": "Paper-based packaging"},
    {"keyword": "cardboard", "esg_signal": "ECO_PREFERRED", "notes": "Cardboard packaging"},
    {"keyword": "refillable", "esg_signal": "ECO_PREFERRED", "notes": "Designed for reuse"},
    {"keyword": "zero waste", "esg_signal": "ECO_PREFERRED", "notes": "Zero-waste product positioning"},

    # --- Plastic indicators ---
    {"keyword": "plastic", "esg_signal": "PLASTIC_LIKELY", "notes": "Generic plastic indicator"},
    {"keyword": "polyethylene", "esg_signal": "PLASTIC_LIKELY", "notes": "Common plastic polymer"},
    {"keyword": "polypropylene", "esg_signal": "PLASTIC_LIKELY", "notes": "Common plastic polymer"},
    {"keyword": "pet", "esg_signal": "PLASTIC_LIKELY", "notes": "Polyethylene terephthalate"},
    {"keyword": "pvc", "esg_signal": "PLASTIC_LIKELY", "notes": "Polyvinyl chloride"},

    # --- Ambiguous / mixed ---
    {"keyword": "eco-friendly", "esg_signal": "MIXED_SIGNALS", "notes": "Marketing term, non-specific"},
    {"keyword": "sustainable", "esg_signal": "MIXED_SIGNALS", "notes": "Broad sustainability claim"},
])

esg_keywords_path = REF_DIR / "esg_keywords.csv"
esg_keywords.to_csv(esg_keywords_path, index=False)

# --------------------------------------------------
# Final confirmation
# --------------------------------------------------
print("✅ Reference tables generated successfully:")
print(" -", category_weight_path)
print(" -", esg_keywords_path)