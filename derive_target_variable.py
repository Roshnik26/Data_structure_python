"""
Derive Target Variable — Convert Cell Viability to Binary Toxicity Label
=========================================================================
Task: Derive target variable from Cell_Viability_pct
Rule: < 60% viability = Toxic (1), >= 60% = Non-toxic (0)

Rationale (cell viability tiers):
  80-100%  → Low/Negligible toxicity   → Non-toxic
  60-80%   → Mild to Moderate toxicity  → Non-toxic
  40-60%   → Significant toxicity       → Toxic
  < 40%    → High toxicity              → Toxic
  Cutoff sits at 60%: below = real toxicity, above = tolerable

Input:  src/data/Harmonized and Range Validated V2 dataset.xlsx  (4288 x 45)
Output: src/data/Target Variable Derived V2 dataset.xlsx  (4288 x 46, 1 new column)

New column:
  - Toxicity_Label : Binary (1 = Toxic, 0 = Non-toxic) based on Cell_Viability_pct < 60%

NOTE: Rows where Cell_Viability_pct is NaN (2104 rows) will get NaN.
      This label is SUPPLEMENTARY to the existing Toxicity_Binary column, not a replacement.

Original PS1 script issues fixed:
  1. Removed incorrect dose ÷1000 conversion (was corrupting in-vitro doses)
  2. Removed incorrect IC50 ÷1000 conversion
  3. Removed cosmetic column renames that broke naming convention
  4. Removed 4-tier column (not needed — only binary required)
  5. Now reads from correct file (Harmonized and Range Validated V2 dataset)
"""

import pandas as pd
import os

# === CONFIG ===
DATA_DIR = os.path.join(os.path.dirname(__file__), "src", "data")
INPUT_FILE = os.path.join(DATA_DIR, "Harmonized and Range Validated V2 dataset.xlsx")
OUTPUT_FILE = os.path.join(DATA_DIR, "Target Variable Derived V2 dataset.xlsx")
VIABILITY_COL = "Cell_Viability_pct"
BINARY_THRESHOLD = 60  # < 60% = Toxic

# === LOAD ===
print(f"Reading: {os.path.basename(INPUT_FILE)}")
df = pd.read_excel(INPUT_FILE, engine="openpyxl")
print(f"Shape: {df.shape}")

if VIABILITY_COL not in df.columns:
    raise ValueError(f"Column '{VIABILITY_COL}' not found. Available: {list(df.columns)}")

# Drop columns from previous run if they exist
for col in ["Toxicity_Label", "Toxicity_Level"]:
    if col in df.columns:
        df.drop(columns=[col], inplace=True)
        print(f"Dropped existing '{col}' column (re-deriving)")

viability = df[VIABILITY_COL]
print(f"\n{VIABILITY_COL}: {viability.notna().sum()} non-null, {viability.isna().sum()} null")

# === DERIVE BINARY LABEL ===
# 1 = Toxic (viability < 60%), 0 = Non-toxic (viability >= 60%), NaN if viability is missing
df["Toxicity_Label"] = pd.NA
mask_valid = viability.notna()
df.loc[mask_valid & (viability < BINARY_THRESHOLD), "Toxicity_Label"] = 1
df.loc[mask_valid & (viability >= BINARY_THRESHOLD), "Toxicity_Label"] = 0

df["Toxicity_Label"] = df["Toxicity_Label"].astype("Int64")

# === SUMMARY ===
print(f"\n{'='*50}")
print(f"Toxicity_Label (threshold: <{BINARY_THRESHOLD}% = Toxic)")
print(f"{'='*50}")
label_counts = df["Toxicity_Label"].value_counts(dropna=False)
for val, count in label_counts.items():
    name = {1: "Toxic", 0: "Non-toxic"}.get(val, "NaN (no viability data)")
    print(f"  {name}: {count}")

# Cross-check with existing Toxicity_Binary
print(f"\n{'='*50}")
print("Cross-check: Viability-derived vs Existing Toxicity_Binary")
print(f"{'='*50}")
label_map = {1: "Toxic(viab)", 0: "NonToxic(viab)"}
df["_temp"] = df["Toxicity_Label"].map(label_map).fillna("No viability")
ct = pd.crosstab(df["Toxicity_Binary"].fillna("NaN"), df["_temp"], margins=True)
print(ct)
df.drop(columns=["_temp"], inplace=True)

# === SAVE ===
print(f"\nShape after: {df.shape}")
df.to_excel(OUTPUT_FILE, index=False, engine="openpyxl")
print(f"Saved to: {os.path.basename(OUTPUT_FILE)}")
print("Done — Toxicity_Label column added")
