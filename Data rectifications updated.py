# %% [markdown]
# # V2 Dataset Preprocessing Pipeline
# 
# **Input:** `version 2 dataset - original.xlsx` (untouched backup, 4,288 x 44)  
# **Output:** `version 2 dataset.xlsx` (cleaned working copy)  
# **Sources:** Mumbai (2,178) + Himadri_M1 (980) + Himadri_M2 (1,130)
# 
# ## All Confirmed Changes
# 1. Case standardization (Q5)
# 2. Drop Source_ID (Q6)
# 3. Fix negative dosage (Q7)
# 4. Remap mislabeled Toxic -> Non-toxic (Q1)
# 5. Mark 55 low-viability Non-toxic rows as Conditional (Q2)
# 6. Keep 191 missing Toxicity_Binary as Unlabeled for semi-supervised (Q3)
# 7. Fix NP_Type - fill 140 missing, 3 categories (Q9)
# 8. Apply Hydrodynamic size threshold 1000nm + Zeta threshold 100mV (Q8)
# 9. Same-label group dedup - remove duplicate rows (Q11)
# 10. Drop Cell_Viability_pct + Label_Viability_Flag (Q4)
# 11. Drop DOI_Reference (Q10)
# 12. Final summary + save

# %%
import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_DIR = Path(r'E:\Python_programs\Work\Project Titan')
DATA_DIR = PROJECT_DIR / 'src' / 'data'

# Always load from the ORIGINAL backup
df = pd.read_excel(DATA_DIR / 'version 2 dataset - original.xlsx', engine='openpyxl')
print(f"Loaded: {df.shape[0]} rows x {df.shape[1]} columns")

# %% [markdown]
# ---
# ## Step 1: Case Standardization (Q5)

# %%
print("BEFORE:")
print(df['Toxicity_Binary'].value_counts(dropna=False))

df['Toxicity_Binary'] = df['Toxicity_Binary'].replace({'TOXIC': 'Toxic'})
df['Toxicity_Label_Original'] = df['Toxicity_Label_Original'].str.lower().str.strip()
df['NP_Subtype'] = df['NP_Subtype'].str.lower().str.strip()
df['Morphology'] = df['Morphology'].str.strip()
df['NP_Type'] = df['NP_Type'].str.strip()

print("\nAFTER:")
print(df['Toxicity_Binary'].value_counts(dropna=False))
print("\n[DONE] Step 1")

# %% [markdown]
# ---
# ## Step 2: Drop Source_ID (Q6)

# %%
print(f"Source_ID non-null: {df['Source_ID'].notna().sum()}")
df.drop(columns=['Source_ID'], inplace=True)
print(f"Dropped Source_ID. Columns: {df.shape[1]}")
print("\n[DONE] Step 2")

# %% [markdown]
# ---
# ## Step 3: Fix Negative Dosage (Q7)

# %%
neg_mask = df['Dose_InVitro_Max_ugmL'] < 0
print(f"Negative dosage rows: {neg_mask.sum()}")
print(df[neg_mask][['Record_ID', 'NP_Name', 'Dose_InVitro_Max_ugmL']].to_string(index=False))

df.loc[neg_mask, 'Dose_InVitro_Max_ugmL'] = np.nan
print(f"\nFixed. Min dosage now: {df['Dose_InVitro_Max_ugmL'].min()}")
print("\n[DONE] Step 3")

# %% [markdown]
# ---
# ## Step 4: Remap Mislabeled Toxic -> Non-toxic (Q1)
# 
# Confirmed: Low cytotoxicity, Negligible cytotoxicity, Non-cytotoxic, Biocompatible -> **Non-toxic**.  
# Mild toxicity -> stays **Toxic**.

# %%
remap_to_nontoxic = [
    'low cytotoxicity', 'negligible cytotoxicity', 'non-cytotoxic',
    'biocompatible', 'non-toxic', 'no cytotoxicity', 'no toxicity',
    'not toxic', 'nontoxic', 'safe', 'low toxicity', 'minimal toxicity',
    'minimal cytotoxicity', 'negligible toxicity',
    'no significant toxicity', 'no significant cytotoxicity',
]

mislabeled_mask = (
    df['Toxicity_Label_Original'].isin(remap_to_nontoxic) &
    (df['Toxicity_Binary'] == 'Toxic')
)

print(f"Mislabeled rows found: {mislabeled_mask.sum()}")
print(df[mislabeled_mask]['Toxicity_Label_Original'].value_counts().to_string())

df.loc[mislabeled_mask, 'Toxicity_Binary'] = 'Non-toxic'

print(f"\nAfter remap:")
print(df['Toxicity_Binary'].value_counts(dropna=False))
print("\n[DONE] Step 4")

# %% [markdown]
# ---
# ## Step 5: Mark Low-Viability Non-toxic as Conditional (Q2)
# 
# 55 rows with Cell_Viability <= 30% but labeled Non-toxic.  
# Sir said: keep as **Conditional** - don't drop, don't flip.

# %%
conflict_mask = (
    df['Label_Viability_Flag'] == 'Conflict_LowViability_Safe'
)
print(f"Low-viability Non-toxic rows: {conflict_mask.sum()}")
print(f"Source breakdown:")
print(df[conflict_mask]['Source'].value_counts().to_string())

df.loc[conflict_mask, 'Toxicity_Binary'] = 'Conditional'

print(f"\nAfter marking:")
print(df['Toxicity_Binary'].value_counts(dropna=False))
print("\n[DONE] Step 5")

# %% [markdown]
# ---
# ## Step 6: Keep Missing Toxicity_Binary as Unlabeled (Q3)
# 
# 191 rows with no label. Sir said: keep for **semi-supervised** learning.  
# Mark as Unlabeled so they are easy to filter during training.

# %%
unlabeled_mask = df['Toxicity_Binary'].isna()
print(f"Rows with missing Toxicity_Binary: {unlabeled_mask.sum()}")

df.loc[unlabeled_mask, 'Toxicity_Binary'] = 'Unlabeled'

print(f"\nAfter marking:")
print(df['Toxicity_Binary'].value_counts(dropna=False))
print("\n[DONE] Step 6")

# %% [markdown]
# ---
# ## Step 7: Fix NP_Type - Fill 140 Missing (Q9)
# 
# Infer from Material_Category. Keep 3 categories: Inorganic / Organic / Hybrid.

# %%
existing_mapping = (
    df[df['NP_Type'].notna()]
    .groupby('Material_Category')['NP_Type']
    .agg(lambda x: x.mode().iloc[0] if len(x) > 0 else np.nan)
)
print("Material_Category -> NP_Type mapping:")
print(existing_mapping.to_string())

missing_mask = df['NP_Type'].isna()
print(f"\nMissing BEFORE: {missing_mask.sum()}")

df.loc[missing_mask, 'NP_Type'] = df.loc[missing_mask, 'Material_Category'].map(existing_mapping)

still_missing = df['NP_Type'].isna().sum()
print(f"Missing AFTER:  {still_missing}")
print(f"Filled: {missing_mask.sum() - still_missing}")
print(f"\nNP_Type distribution:")
print(df['NP_Type'].value_counts(dropna=False))
print("\n[DONE] Step 7")

# %% [markdown]
# ---
# ## Step 8: Apply Thresholds (Q8)
# 
# Confirmed by sir:  
# - Hydrodynamic_Size_nm > 1000nm -> NaN  
# - |Zeta_Potential_mV| > 100 -> NaN

# %%
# Hydrodynamic size
hydro_mask = df['Hydrodynamic_Size_nm'] > 1000
print(f"Hydrodynamic > 1000nm: {hydro_mask.sum()} rows")
df.loc[hydro_mask, 'Hydrodynamic_Size_nm'] = np.nan
print(f"Max after cap: {df['Hydrodynamic_Size_nm'].max()}")

# Zeta potential
zeta_mask = df['Zeta_Potential_mV'].abs() > 100
print(f"\n|Zeta| > 100mV: {zeta_mask.sum()} rows")
if zeta_mask.sum() > 0:
    print(df.loc[zeta_mask, ['Record_ID', 'NP_Name', 'Zeta_Potential_mV']].to_string(index=False))
df.loc[zeta_mask, 'Zeta_Potential_mV'] = np.nan
print(f"Max after cap: {df['Zeta_Potential_mV'].max()}")
print(f"Min after cap: {df['Zeta_Potential_mV'].min()}")
print("\n[DONE] Step 8")

# %% [markdown]
# ---
# ## Step 9: Same-Label Group Dedup (Q11)
# 
# Groups by (DOI_Reference, NP_Name, Cell_Lines) with 2+ rows.  
# If all rows in a group have the **same** Toxicity_Binary -> keep 1, drop rest.  
# Mixed-label groups are left untouched.

# %%
before_count = len(df)

has_doi = df['DOI_Reference'].notna()
df_doi = df[has_doi].copy()
df_no_doi = df[~has_doi].copy()

group_cols = ['DOI_Reference', 'NP_Name', 'Cell_Lines']

def is_same_label(group):
    labels = group['Toxicity_Binary'].unique()
    real_labels = [l for l in labels if l not in ('Unlabeled', 'Conditional')]
    return len(set(real_labels)) <= 1

groups = df_doi.groupby(group_cols)
rows_to_drop = []
same_label_groups = 0
mixed_label_groups = 0

for name, group in groups:
    if len(group) < 2:
        continue
    if is_same_label(group):
        same_label_groups += 1
        rows_to_drop.extend(group.index[1:].tolist())
    else:
        mixed_label_groups += 1

print(f"Same-label groups: {same_label_groups}")
print(f"Mixed-label groups: {mixed_label_groups} (untouched)")
print(f"Rows to remove: {len(rows_to_drop)}")

df_doi_deduped = df_doi.drop(index=rows_to_drop)
df = pd.concat([df_doi_deduped, df_no_doi], ignore_index=True)

after_count = len(df)
print(f"\nBefore: {before_count} rows")
print(f"After:  {after_count} rows")
print(f"Removed: {before_count - after_count} rows")
print(f"\nToxicity_Binary after dedup:")
print(df['Toxicity_Binary'].value_counts(dropna=False))
print("\n[DONE] Step 9")

# %% [markdown]
# ---
# ## Step 10: Drop Cell_Viability_pct + Label_Viability_Flag (Q4)

# %%
cols_to_drop = ['Cell_Viability_pct', 'Label_Viability_Flag']
print(f"Before: {df.shape[1]} columns")
for col in cols_to_drop:
    if col in df.columns:
        print(f"  Dropping '{col}'")
df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)
print(f"After: {df.shape[1]} columns")
print("\n[DONE] Step 10")

# %% [markdown]
# ---
# ## Step 11: Drop DOI_Reference (Q10)

# %%
if 'DOI_Reference' in df.columns:
    print(f"Dropping DOI_Reference ({df['DOI_Reference'].notna().sum()} non-null)")
    df.drop(columns=['DOI_Reference'], inplace=True)
print(f"Columns: {df.shape[1]}")
print("\n[DONE] Step 11")

# %% [markdown]
# ---
# ## Step 12: Final Summary + Save

# %%
print("=" * 60)
print("  PREPROCESSING SUMMARY")
print("=" * 60)
print(f"\nDataset: {df.shape[0]} rows x {df.shape[1]} columns")

print(f"\n--- Columns ({df.shape[1]}) ---")
for i, col in enumerate(df.columns, 1):
    non_null = df[col].notna().sum()
    pct = non_null / len(df) * 100
    print(f"  {i:2d}. {col:<35s} {str(df[col].dtype):<10s} {non_null:,}/{len(df):,} ({pct:.1f}%)")

print(f"\n--- Changes Applied ---")
print(f"  [DONE] Case standardization")
print(f"  [DONE] Dropped Source_ID")
print(f"  [DONE] Fixed negative dosage -> NaN")
print(f"  [DONE] Remapped mislabeled Toxic -> Non-toxic")
print(f"  [DONE] Marked conflict rows as Conditional")
print(f"  [DONE] Marked missing labels as Unlabeled (semi-supervised)")
print(f"  [DONE] Filled NP_Type from Material_Category")
print(f"  [DONE] Hydrodynamic > 1000nm -> NaN")
print(f"  [DONE] |Zeta| > 100mV -> NaN")
print(f"  [DONE] Same-label group dedup")
print(f"  [DONE] Dropped Cell_Viability_pct, Label_Viability_Flag")
print(f"  [DONE] Dropped DOI_Reference")

print(f"\n--- Toxicity_Binary ---")
print(df['Toxicity_Binary'].value_counts(dropna=False).to_string())

print(f"\n--- NP_Type ---")
print(df['NP_Type'].value_counts(dropna=False).to_string())

print(f"\n--- Top 15 Missing ---")
missing = df.isnull().sum().sort_values(ascending=False)
for col in missing.head(15).index:
    if missing[col] > 0:
        pct = missing[col] / len(df) * 100
        print(f"  {col:<35s} {missing[col]:>5d} ({pct:.1f}%)")

# %%
output_path = DATA_DIR / 'version 2 dataset.xlsx'
df.to_excel(output_path, index=False, engine='openpyxl')

print(f"Saved: {output_path}")
print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
print(f"Original untouched at: {DATA_DIR / 'version 2 dataset - original.xlsx'}")
print("\n[DONE] Pipeline complete.")


