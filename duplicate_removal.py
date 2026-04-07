import pandas as pd
import numpy as np
from pathlib import Path
import warnings

# Suppress openpyxl warnings if any
warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# Define directory based on local structure
DATA_DIR = Path(r'C:\Users\admin\OneDrive\Desktop\Data_structure_python')
INPUT_FILE = DATA_DIR / 'Target Variable Derived V2 dataset.xlsx'
OUTPUT_FILE = DATA_DIR / 'Deduplicated V2 dataset.xlsx'

print(f"Loading dataset from: {INPUT_FILE}")
df = pd.read_excel(INPUT_FILE, engine='openpyxl')
initial_rows = len(df)
print(f"Start: {initial_rows:,} rows")

# ==========================================
# STEP 1: DOI-Based Deduplication
# ==========================================
print("\n--- STEP 1: DOI-Based Deduplication ---")

# Ensure DOI_Reference exists before proceeding
if 'DOI_Reference' in df.columns:
    group_cols = ['DOI_Reference', 'NP_Name', 'Dose_InVitro_Max_ugmL', 'Cell_Lines', 'Exposure_Time_h']
    
    # Filter available grouping columns (in case some are missing in actual file)
    group_cols = [c for c in group_cols if c in df.columns]
    print(f"Grouping columns for deduplication: {group_cols}")
    
    # Separate rows with DOI and without DOI
    has_doi = df['DOI_Reference'].notna()
    df_doi = df[has_doi].copy()
    df_no_doi = df[~has_doi].copy()
    
    # Identify duplicate groups
    groups = df_doi.groupby(group_cols, dropna=False)
    
    rows_to_drop = []
    
    for name, group in groups:
        if len(group) < 2:
            continue
            
        # We have multiple rows for the same experimental setup
        if 'Toxicity_Binary' in group.columns:
            tox_labels = group['Toxicity_Binary'].dropna().unique()
            
            # If all rows have the same label (or only one label is present)
            if len(tox_labels) <= 1:
                # Keep the first, drop the rest
                rows_to_drop.extend(group.index[1:].tolist())
            else:
                # Mixed labels! Keep the TOXIC row(s), pick the first one, drop the rest.
                # Find indices where Toxicity_Binary is 'Toxic' (case-insensitive if needed, assuming 'Toxic')
                toxic_idx = group[group['Toxicity_Binary'].str.lower() == 'toxic'].index
                if len(toxic_idx) > 0:
                    keep_idx = toxic_idx[0]
                else:
                    keep_idx = group.index[0] # Fallback
                
                # Drop all indices in group except the one we keep
                drop_indices = [idx for idx in group.index if idx != keep_idx]
                rows_to_drop.extend(drop_indices)
        else:
            # If Toxicity_Binary is not present, just keep the first
            rows_to_drop.extend(group.index[1:].tolist())
            
    print(f"Identified {len(rows_to_drop)} duplicate rows to remove in groups with DOI.")
    
    # Drop from the DOI DataFrame
    df_doi_deduped = df_doi.drop(index=rows_to_drop)
    
    # Recombine
    df = pd.concat([df_doi_deduped, df_no_doi], ignore_index=True)
else:
    print("WARNING: 'DOI_Reference' column not found. Skipping Step 1.")

rows_after_step1 = len(df)
print(f"After Step 1: {rows_after_step1:,} rows (Removed {initial_rows - rows_after_step1:,})")

# ==========================================
# STEP 2: Drop Rows with No Dose AND No Toxicity
# ==========================================
print("\n--- STEP 2: Drop Rows with No Dose AND No Toxicity ---")

dose_cols_to_check = ['Dose_InVitro_Min_ugmL', 'Dose_InVitro_Max_ugmL', 'Dose_InVivo_mgkg']
tox_cols_to_check = ['Toxicity_Binary', 'Toxicity_Label']

# Filter to actual columns
actual_dose_cols = [c for c in dose_cols_to_check if c in df.columns]
actual_tox_cols = [c for c in tox_cols_to_check if c in df.columns]

# Initialize masks as all True (meaning "are all NaN?")
# If no dose columns exist, we assume dose is NaN = True. But let's only do it if columns exist.
if actual_dose_cols and actual_tox_cols:
    all_doses_na = df[actual_dose_cols].isna().all(axis=1)
    all_tox_na = df[actual_tox_cols].isna().all(axis=1)
    
    drop_mask = all_doses_na & all_tox_na
    rows_to_drop_step2 = drop_mask.sum()
    
    # Drop them
    df = df[~drop_mask].reset_index(drop=True)
else:
    rows_to_drop_step2 = 0
    print("WARNING: Required dose or toxicity columns missing. Skipping Step 2.")

rows_after_step2 = len(df)
print(f"Identified {rows_to_drop_step2} rows with no dose AND no toxicity data to drop.")
print(f"After Step 2: {rows_after_step2:,} rows (Removed {rows_after_step1 - rows_after_step2:,})")

# ==========================================
# SUMMARY & EXPORT
# ==========================================
print("\n--- SUMMARY ---")
print(f"Start:          {initial_rows:>6,} rows")
print(f"After Step 1:   {rows_after_step1:>6,} rows  (-{initial_rows - rows_after_step1})")
print(f"After Step 2:   {rows_after_step2:>6,} rows  (-{rows_after_step1 - rows_after_step2})")
print(f"Total removed:  {initial_rows - rows_after_step2:>6,} rows ({(initial_rows - rows_after_step2)/initial_rows*100:.1f}%)")

print(f"\nSaving to: {OUTPUT_FILE}")
df.to_excel(OUTPUT_FILE, index=False, engine='openpyxl')
print("[DONE] Deduplication completely finished!")
