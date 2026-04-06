"""
Unit Harmonization Script for V2 Dataset
=========================================
Input:  version 2 dataset - original.xlsx (4,288 rows x 44 columns)
Output: Unit Harmonized V2 dataset.xlsx

Rules:
  - In vitro doses -> ug/mL (already correct in original, kept as-is)
  - In vivo doses  -> mg/kg (parsed from Dose_InVivo_Notes free text)

What this script does:
  1. Keeps Dose_InVitro_Min_ugmL and Dose_InVitro_Max_ugmL unchanged (already ug/mL)
  2. Parses Dose_InVivo_Notes to extract numeric dose in mg/kg
  3. Applies unit conversions where needed (mg/L -> mg/kg, g/kg -> mg/kg, etc.)
  4. Creates a new column: Dose_InVivo_mgkg (max dose extracted)
  5. Saves as "Unit Harmonized V2 dataset.xlsx"
"""

import pandas as pd
import numpy as np
import re
from pathlib import Path

PROJECT_DIR = Path(r'E:\Python_programs\Work\Project Titan')
DATA_DIR = PROJECT_DIR / 'src' / 'data'

# ── Load original ────────────────────────────────────────────────────────────
df = pd.read_excel(DATA_DIR / 'version 2 dataset - original.xlsx', engine='openpyxl')
print(f"Loaded: {df.shape[0]} rows x {df.shape[1]} columns")

# ── Conversion factors TO mg/kg ──────────────────────────────────────────────
# Assumption: for liquid concentrations (mg/L, ug/mL, etc.) where body weight
# is not specified, we note them but cannot convert to mg/kg without knowing
# the administered volume and animal weight. We only convert units that are
# mass-per-mass (weight-based dosing).
#
# Directly convertible to mg/kg:
#   mg/kg  -> 1.0
#   g/kg   -> 1000.0
#   ug/kg  -> 0.001
#
# NOT directly convertible (concentration units, need volume + weight info):
#   mg/L, mg/mL, ug/mL, ug/L, ppm, nM, uM, mM
#   These are kept as-is with a flag column indicating the original unit.

UNIT_TO_MGKG = {
    'mg/kg': 1.0,
    'mg kg-1': 1.0,
    'mg/kg bw': 1.0,
    'mg/kg/day': 1.0,
    'mg/kg bw/day': 1.0,
    'mg/kg body weight': 1.0,
    'g/kg': 1000.0,
    'g kg-1': 1000.0,
    'ug/kg': 0.001,
    'µg/kg': 0.001,
}

# Units we can detect but NOT convert to mg/kg
CONCENTRATION_UNITS = {
    'mg/l', 'mg l-1', 'mg/ml', 'ug/ml', 'µg/ml', 'ug/l', 'µg/l',
    'ppm', 'nm', 'um', 'µm', 'mm', 'mg', 'ug', 'µg',
    'mg/m2', 'mg/m3', 'ml/kg',
}


def extract_dose_mgkg(text):
    """
    Extract the maximum numeric dose from free-text and convert to mg/kg.
    Returns (dose_mgkg, original_unit, parse_status)
    """
    if pd.isna(text) or not isinstance(text, str):
        return np.nan, None, 'no_data'

    text_clean = text.strip().lower()

    # Skip non-numeric entries
    if text_clean in ('not reported', 'na', 'n/a', 'nr', 'not specified',
                      'not numerically specified', 'discussed in cited works only'):
        return np.nan, None, 'not_reported'

    # Try to find mg/kg-type units first (weight-based dosing)
    # Pattern: number followed by unit
    best_dose = np.nan
    best_unit = None
    status = 'no_match'

    # Normalize unicode
    text_norm = text_clean.replace('µ', 'u').replace('–', '-').replace('—', '-')
    text_norm = text_norm.replace('\u2212', '-')  # minus sign
    text_norm = text_norm.replace('kg-1', '/kg').replace('l-1', '/l')

    # Try mg/kg variants first (preferred unit)
    mgkg_patterns = [
        r'([\d.]+)\s*(?:mg/kg(?:\s*bw)?(?:/day)?|mg\s*kg\s*[-/]?\s*1)',
    ]

    for pattern in mgkg_patterns:
        matches = re.findall(pattern, text_norm)
        if matches:
            values = [float(m) for m in matches if m]
            if values:
                best_dose = max(values)  # take max dose
                best_unit = 'mg/kg'
                status = 'converted'

    # If no mg/kg found, try g/kg
    if pd.isna(best_dose):
        gkg_matches = re.findall(r'([\d.]+)\s*g/kg', text_norm)
        if gkg_matches:
            values = [float(m) for m in gkg_matches]
            best_dose = max(values) * 1000  # g/kg -> mg/kg
            best_unit = 'g/kg'
            status = 'converted'

    # Try ug/kg
    if pd.isna(best_dose):
        ugkg_matches = re.findall(r'([\d.]+)\s*ug/kg', text_norm)
        if ugkg_matches:
            values = [float(m) for m in ugkg_matches]
            best_dose = max(values) * 0.001  # ug/kg -> mg/kg
            best_unit = 'ug/kg'
            status = 'converted'

    # If still nothing, try to extract concentration units (not convertible but recorded)
    if pd.isna(best_dose):
        conc_patterns = [
            (r'([\d.]+)\s*mg/l', 'mg/L'),
            (r'([\d.]+)\s*mg/ml', 'mg/mL'),
            (r'([\d.]+)\s*ug/ml', 'ug/mL'),
            (r'([\d.]+)\s*ppm', 'ppm'),
            (r'([\d.]+)\s*um\b', 'uM'),
            (r'([\d.]+)\s*mm\b', 'mM'),
            (r'([\d.]+)\s*nm\b', 'nM'),
            (r'([\d.]+)\s*mg\b', 'mg'),
            (r'([\d.]+)\s*ug\b', 'ug'),
        ]
        for pattern, unit in conc_patterns:
            matches = re.findall(pattern, text_norm)
            if matches:
                values = [float(m) for m in matches]
                best_dose = max(values)
                best_unit = unit
                status = 'not_convertible'
                break

    # Last resort: just grab any number
    if pd.isna(best_dose):
        any_numbers = re.findall(r'(\d+\.?\d*)', text_norm)
        if any_numbers:
            values = [float(n) for n in any_numbers if n.strip('.') and float(n) > 0]
            if values:
                best_dose = max(values)
                best_unit = 'unknown'
                status = 'number_only'

    return best_dose, best_unit, status


# ── Apply extraction ─────────────────────────────────────────────────────────
print("\nExtracting in vivo doses from Dose_InVivo_Notes...")

results = df['Dose_InVivo_Notes'].apply(extract_dose_mgkg)

df['Dose_InVivo_Extracted'] = results.apply(lambda x: x[0])
df['Dose_InVivo_Original_Unit'] = results.apply(lambda x: x[1])
df['Dose_InVivo_Parse_Status'] = results.apply(lambda x: x[2])

# Create the final mg/kg column (only for rows that were actually converted to mg/kg)
df['Dose_InVivo_mgkg'] = np.where(
    df['Dose_InVivo_Parse_Status'] == 'converted',
    df['Dose_InVivo_Extracted'],
    np.nan
)

# ── Report ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("  UNIT HARMONIZATION REPORT")
print("=" * 60)

print(f"\n--- In Vitro Doses (unchanged, already in ug/mL) ---")
print(f"  Dose_InVitro_Min_ugmL: {df['Dose_InVitro_Min_ugmL'].notna().sum()} non-null")
print(f"  Dose_InVitro_Max_ugmL: {df['Dose_InVitro_Max_ugmL'].notna().sum()} non-null")
vitro_desc = df['Dose_InVitro_Max_ugmL'].describe()
print(f"  Range: {vitro_desc['min']:.2f} - {vitro_desc['max']:.2f} ug/mL")

print(f"\n--- In Vivo Dose Extraction Results ---")
status_counts = df['Dose_InVivo_Parse_Status'].value_counts()
for status, count in status_counts.items():
    pct = count / len(df) * 100
    print(f"  {status:<20s} {count:>5d} ({pct:.1f}%)")

print(f"\n--- Original Units Found in In Vivo Notes ---")
unit_counts = df['Dose_InVivo_Original_Unit'].value_counts(dropna=False)
for unit, count in unit_counts.items():
    if pd.notna(unit):
        print(f"  {unit:<15s} {count:>5d}")

print(f"\n--- Dose_InVivo_mgkg (converted to mg/kg) ---")
mgkg_valid = df['Dose_InVivo_mgkg'].notna().sum()
print(f"  Successfully converted: {mgkg_valid} rows")
if mgkg_valid > 0:
    desc = df['Dose_InVivo_mgkg'].describe()
    print(f"  Range: {desc['min']:.2f} - {desc['max']:.2f} mg/kg")
    print(f"  Median: {desc['50%']:.2f} mg/kg")
    print(f"  Mean: {desc['mean']:.2f} mg/kg")

print(f"\n--- Not Convertible to mg/kg (concentration units) ---")
not_conv = df[df['Dose_InVivo_Parse_Status'] == 'not_convertible']
print(f"  Rows: {len(not_conv)}")
if len(not_conv) > 0:
    print(f"  These have doses in concentration units (mg/L, ug/mL, ppm, etc.)")
    print(f"  that cannot be converted to mg/kg without volume/weight info.")
    print(f"  Unit breakdown:")
    print(not_conv['Dose_InVivo_Original_Unit'].value_counts().to_string())

# ── Verify in vitro columns are untouched ────────────────────────────────────
print(f"\n--- Verification: In Vitro Columns Untouched ---")
df_orig = pd.read_excel(DATA_DIR / 'version 2 dataset - original.xlsx',
                        engine='openpyxl',
                        usecols=['Dose_InVitro_Min_ugmL', 'Dose_InVitro_Max_ugmL'])
min_match = df['Dose_InVitro_Min_ugmL'].equals(df_orig['Dose_InVitro_Min_ugmL'])
max_match = df['Dose_InVitro_Max_ugmL'].equals(df_orig['Dose_InVitro_Max_ugmL'])
print(f"  Dose_InVitro_Min_ugmL unchanged: {min_match}")
print(f"  Dose_InVitro_Max_ugmL unchanged: {max_match}")

# ── Save ─────────────────────────────────────────────────────────────────────
# Drop intermediate columns, keep only the final result
cols_to_drop = ['Dose_InVivo_Extracted', 'Dose_InVivo_Original_Unit', 'Dose_InVivo_Parse_Status']
df_output = df.drop(columns=cols_to_drop)

output_path = DATA_DIR / 'Unit Harmonized V2 dataset.xlsx'
df_output.to_excel(output_path, index=False, engine='openpyxl')

print(f"\n--- Output ---")
print(f"  Saved: {output_path}")
print(f"  Shape: {df_output.shape[0]} rows x {df_output.shape[1]} columns")
print(f"\n  New column added: Dose_InVivo_mgkg")
print(f"  In vitro columns: Dose_InVitro_Min_ugmL, Dose_InVitro_Max_ugmL (unchanged)")
print(f"\n[DONE] Unit harmonization complete.")
