# %% [markdown]
# # V2 Dataset Preprocessing Pipeline
# 
# **Dataset:** `version 2 dataset.xlsx` (4,288 rows x 44 columns)  
# **Backup:** `version 2 dataset - original.xlsx` (untouched)  
# **Sources:** Mumbai (2,178) + Himadri_M1 (980) + Himadri_M2 (1,130)
# 
# ## Processing Order
# 1. Case standardization (Q5)
# 2. Drop redundant columns — Source_ID, DOI (Q6, Q10)
# 3. Fix negative dosage to NaN (Q7)
# 4. Remap binary toxicity labels (Q1)
# 5. Fix NP_Type — fill 140 missing, keep 3 categories (Q9)
# 6. Apply size/zeta thresholds (Q8) — tentative, pending sir's input
# 7. Handle near-duplicates (Q11) — pending sir's input
# 8. Drop Cell_Viability_pct (Q4) — last step
# 9. Flag rows for Q2, Q3 — no changes, pending sir's input
# 
# **Note:** Steps 6, 7, 9 are marked pending — we apply tentative logic now and will revise after sir's feedback.

# %%
# Cell 1: Imports and load dataset
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_DIR = Path(r"E:\Python_programs\Work\Project Titan")
DATA_DIR = PROJECT_DIR / "src" / "data"

# Load working copy
df = pd.read_excel(DATA_DIR / "version 2 dataset.xlsx", engine="openpyxl")
print(f"Loaded: {df.shape[0]} rows x {df.shape[1]} columns")
print(f"Columns:\n{list(df.columns)}")
print("Dtypes summary:")
print(df.dtypes.value_counts())

# %% [markdown]
# ---
# ## Step 1: Case Standardization (Q5)
# 
# Standardize case inconsistencies in:
# - `Toxicity_Binary`: "TOXIC" -> "Toxic"
# - `Toxicity_Label_Original`: normalize to lowercase
# - `NP_Subtype`: normalize to lowercase

# %%
# Cell 2: Case standardization — BEFORE
print("=== BEFORE Case Standardization ===")
print(f"\nToxicity_Binary value_counts:")
print(df['Toxicity_Binary'].value_counts(dropna=False))
print(f"\nToxicity_Label_Original unique count: {df['Toxicity_Label_Original'].nunique()}")
print(f"Sample duplicates from case issues:")
orig_labels = df['Toxicity_Label_Original'].dropna()
# Find labels that differ only by case
lower_groups = orig_labels.str.lower().value_counts()
actual_groups = orig_labels.value_counts()
print(f"  Unique before lowercase: {len(actual_groups)}")
print(f"  Unique after lowercase:  {len(lower_groups)}")
print(f"  Case duplicates eliminated: {len(actual_groups) - len(lower_groups)}")
print(f"\nNP_Subtype unique count: {df['NP_Subtype'].nunique()}")
nps = df['NP_Subtype'].dropna()
print(f"  Unique before lowercase: {nps.nunique()}")
print(f"  Unique after lowercase:  {nps.str.lower().nunique()}")
print(f"  Case duplicates eliminated: {nps.nunique() - nps.str.lower().nunique()}")

# %%
# Cell 3: Apply case standardization
# Toxicity_Binary: TOXIC -> Toxic (keep Non-toxic as is)
df['Toxicity_Binary'] = df['Toxicity_Binary'].replace({'TOXIC': 'Toxic'})

# Toxicity_Label_Original: lowercase
df['Toxicity_Label_Original'] = df['Toxicity_Label_Original'].str.lower().str.strip()

# NP_Subtype: lowercase
df['NP_Subtype'] = df['NP_Subtype'].str.lower().str.strip()

# Morphology: title case for consistency
df['Morphology'] = df['Morphology'].str.strip()

# NP_Type: title case
df['NP_Type'] = df['NP_Type'].str.strip()

print("=== AFTER Case Standardization ===")
print(f"\nToxicity_Binary value_counts:")
print(df['Toxicity_Binary'].value_counts(dropna=False))
print(f"\nToxicity_Label_Original unique count: {df['Toxicity_Label_Original'].nunique()}")
print(f"NP_Subtype unique count: {df['NP_Subtype'].nunique()}")
print("\n[DONE] Step 1 complete")

# %% [markdown]
# ---
# ## Step 2: Drop Redundant Columns (Q6, Q10)
# 
# - `Source_ID`: 100% empty, confirmed redundant by Yash
# - `DOI_Reference`: Dropping as column, but using it for dedup analysis first (Step 7)

# %%
# Cell 4: Drop Source_ID (100% empty)
# Keep DOI_Reference in memory for Step 7 dedup analysis, but mark for final drop
print(f"Before drop: {df.shape[1]} columns")
print(f"Source_ID non-null count: {df['Source_ID'].notna().sum()}")

df.drop(columns=['Source_ID'], inplace=True)

print(f"After drop: {df.shape[1]} columns")
print(f"Dropped: Source_ID")
print("\nNote: DOI_Reference kept temporarily for dedup analysis in Step 7.")
print("      Will be dropped at the end.")
print("\n[DONE] Step 2 complete")

# %% [markdown]
# ---
# ## Step 3: Fix Negative Dosage (Q7)
# 
# 2 rows in `Dose_InVitro_Max_ugmL` have negative values (min = -29). Set to NaN for later imputation.

# %%
# Cell 5: Fix negative dosage values
neg_mask = df['Dose_InVitro_Max_ugmL'] < 0
neg_rows = df[neg_mask][['Record_ID', 'NP_Name', 'Dose_InVitro_Max_ugmL', 'Source']]

print(f"Rows with negative dosage: {neg_mask.sum()}")
print(f"\nAffected rows:")
print(neg_rows.to_string(index=False))

# Set to NaN
df.loc[neg_mask, 'Dose_InVitro_Max_ugmL'] = np.nan

print(f"\nAfter fix — min dosage: {df['Dose_InVitro_Max_ugmL'].min()}")
print(f"Negative values remaining: {(df['Dose_InVitro_Max_ugmL'] < 0).sum()}")
print("\n[DONE] Step 3 complete")

# %% [markdown]
# ---
# ## Step 4: Remap Binary Toxicity Labels (Q1)
# 
# Tech lead confirmed:
# - "Low cytotoxicity", "Negligible cytotoxicity", "Non-cytotoxic", "Biocompatible" -> **Non-toxic**
# - "Mild toxicity" -> **Toxic** (stays as is)
# 
# We also check the `Label_Viability_Flag` conflicts after remapping.

# %%
# Cell 6: Identify mislabeled rows — BEFORE remapping
# These original labels should NOT be Toxic
remap_to_nontoxic = [
    'low cytotoxicity',
    'negligible cytotoxicity', 
    'non-cytotoxic',
    'biocompatible',
    'non-toxic',           # edge case: labeled 'non-toxic' in original but mapped to Toxic
    'no cytotoxicity',
    'no toxicity',
    'not toxic',
    'nontoxic',
    'safe',
    'low toxicity',
    'minimal toxicity',
    'minimal cytotoxicity',
    'negligible toxicity',
    'no significant toxicity',
    'no significant cytotoxicity',
]

# Find rows where original label suggests non-toxic but binary says Toxic
mislabeled_mask = (
    df['Toxicity_Label_Original'].isin(remap_to_nontoxic) & 
    (df['Toxicity_Binary'] == 'Toxic')
)

print(f"Rows with Toxic label but non-toxic original description: {mislabeled_mask.sum()}")
print(f"\nBreakdown by original label:")
mislabeled = df[mislabeled_mask]['Toxicity_Label_Original'].value_counts()
print(mislabeled.to_string())

print(f"\nBreakdown by Source:")
print(df[mislabeled_mask]['Source'].value_counts().to_string())

print(f"\n--- Current Toxicity_Binary distribution ---")
print(df['Toxicity_Binary'].value_counts(dropna=False))

# %%
# Cell 7: Apply remapping
df.loc[mislabeled_mask, 'Toxicity_Binary'] = 'Non-toxic'

print("=== AFTER Remapping ===")
print(f"Rows remapped from Toxic to Non-toxic: {mislabeled_mask.sum()}")
print(f"\nNew Toxicity_Binary distribution:")
print(df['Toxicity_Binary'].value_counts(dropna=False))

# Also check: rows with viability = 100% still labeled Toxic?
if 'Cell_Viability_pct' in df.columns:
    still_100_toxic = (
        (df['Cell_Viability_pct'] == 100) & 
        (df['Toxicity_Binary'] == 'Toxic')
    ).sum()
    print(f"\nRows with 100% viability still labeled Toxic: {still_100_toxic}")
    
    # Show remaining conflict counts
    high_viab_toxic = (
        (df['Cell_Viability_pct'] >= 70) & 
        (df['Toxicity_Binary'] == 'Toxic')
    ).sum()
    print(f"Rows with viability >= 70% still labeled Toxic: {high_viab_toxic}")
    print("(These may need further review — some are legitimate IC50-based labels)")

print("\n[DONE] Step 4 complete")

# %% [markdown]
# ---
# ## Step 5: Fix NP_Type — Fill Missing + Keep 3 Categories (Q9)
# 
# Infer NP_Type from `Material_Category` for the 140 missing rows.
# - Metals, metal oxides, quantum dots, carbon-based -> Inorganic
# - Liposomes, dendrimers, polymeric, protein-based -> Organic
# - Polymer-metal composites, functionalized combos -> Hybrid
# 
# Keep 3 categories: Inorganic / Organic / Hybrid (agreed with Yash).

# %%
# Cell 8: Analyze missing NP_Type rows
missing_npt = df[df['NP_Type'].isna()]

print(f"Rows with missing NP_Type: {len(missing_npt)}")
print(f"\nMaterial_Category distribution for missing NP_Type rows:")
print(missing_npt['Material_Category'].value_counts().to_string())
print(f"\nSource distribution:")
print(missing_npt['Source'].value_counts().to_string())
print(f"\nNP_Name samples:")
print(missing_npt['NP_Name'].value_counts().head(15).to_string())

# %%
# Cell 9: Build Material_Category -> NP_Type mapping and apply
# First, check what mappings already exist in the data for non-null rows
existing_mapping = (
    df[df['NP_Type'].notna()]
    .groupby('Material_Category')['NP_Type']
    .agg(lambda x: x.mode().iloc[0] if len(x) > 0 else np.nan)
)
print("Existing Material_Category -> NP_Type mappings from data:")
print(existing_mapping.to_string())
print(f"\nTotal mapped categories: {len(existing_mapping)}")

# %%
# Cell 10: Apply NP_Type inference for missing rows
# Use the existing mapping from the dataset itself
missing_mask = df['NP_Type'].isna()
missing_categories = df.loc[missing_mask, 'Material_Category']

# Map using existing data patterns
df.loc[missing_mask, 'NP_Type'] = missing_categories.map(existing_mapping)

# Check results
still_missing = df['NP_Type'].isna().sum()
print(f"NP_Type missing BEFORE: {missing_mask.sum()}")
print(f"NP_Type missing AFTER:  {still_missing}")
print(f"Filled: {missing_mask.sum() - still_missing}")

if still_missing > 0:
    print(f"\nStill missing — these Material_Categories had no existing mapping:")
    print(df[df['NP_Type'].isna()]['Material_Category'].value_counts().to_string())

print(f"\nFinal NP_Type distribution:")
print(df['NP_Type'].value_counts(dropna=False))
print("\n[DONE] Step 5 complete")

# %% [markdown]
# ---
# ## Step 6: Apply Size/Zeta Thresholds (Q8) — TENTATIVE
# 
# **Pending sir's input.** Using tentative thresholds for now:
# - `Hydrodynamic_Size_nm` > 1000 -> set to NaN (Yash's suggestion)
# - `Zeta_Potential_mV` outlier at 134 -> flagged, not changed yet
# 
# These may be revised after expert consultation.

# %%
# Cell 11: Hydrodynamic size threshold
HYDRO_THRESHOLD = 1000  # nm — tentative, pending sir's input

over_threshold = df['Hydrodynamic_Size_nm'] > HYDRO_THRESHOLD
print(f"Hydrodynamic_Size_nm > {HYDRO_THRESHOLD}nm: {over_threshold.sum()} rows")
print(f"\nDistribution of over-threshold values:")
print(df.loc[over_threshold, 'Hydrodynamic_Size_nm'].describe())
print(f"\nSample over-threshold rows:")
print(df.loc[over_threshold, ['Record_ID', 'NP_Name', 'Hydrodynamic_Size_nm', 'Source']].head(10).to_string(index=False))

# Apply threshold — set to NaN
df.loc[over_threshold, 'Hydrodynamic_Size_nm'] = np.nan
print(f"\nAfter capping: max Hydrodynamic_Size_nm = {df['Hydrodynamic_Size_nm'].max()}")
print(f"Rows set to NaN: {over_threshold.sum()}")

# %%
# Cell 12: Zeta potential outlier check
ZETA_THRESHOLD = 100  # mV — tentative

zeta_outliers = df['Zeta_Potential_mV'].abs() > ZETA_THRESHOLD
print(f"Zeta_Potential_mV with |value| > {ZETA_THRESHOLD}: {zeta_outliers.sum()} rows")
if zeta_outliers.sum() > 0:
    print(f"\nOutlier rows:")
    print(df.loc[zeta_outliers, ['Record_ID', 'NP_Name', 'Zeta_Potential_mV', 'Source']].to_string(index=False))

# FLAG only — not changing until sir confirms
print(f"\n[NOTE] Zeta outliers flagged but NOT modified — pending sir's input.")
print(f"       Change the line below to apply the threshold if approved.")

# Uncomment the next 2 lines after sir confirms the threshold:
# df.loc[zeta_outliers, 'Zeta_Potential_mV'] = np.nan
# print(f"Applied: {zeta_outliers.sum()} zeta values set to NaN")

print("\n[DONE] Step 6 complete")

# %% [markdown]
# ---
# ## Step 7: Near-Duplicate Analysis (Q11) — PENDING
# 
# DOI-based dedup check. Identifying true duplicates (same DOI + NP + cell line + viability) vs legitimate repeats (different experimental conditions).
# 
# **Action pending sir's input** — Yash suggests averaging, Rishith prefers DOI-based pick-one. Analysis below to inform the decision.

# %%
# Cell 13: Near-duplicate analysis
# Level 1: Exact same DOI + NP_Name + Cell_Lines (same paper, same NP, same cell line)
dedup_cols_l1 = ['DOI_Reference', 'NP_Name', 'Cell_Lines']
df_with_doi = df[df['DOI_Reference'].notna()].copy()

l1_dupes = df_with_doi.duplicated(subset=dedup_cols_l1, keep=False)
l1_groups = df_with_doi[l1_dupes].groupby(dedup_cols_l1).size()

print("=== Level 1: Same DOI + NP_Name + Cell_Lines ===")
print(f"Duplicate rows: {l1_dupes.sum()}")
print(f"Duplicate groups: {len(l1_groups)}")
print(f"\nGroup size distribution:")
print(l1_groups.value_counts().sort_index().to_string())

# Level 2: Same DOI + NP_Name + Cell_Lines + similar Toxicity_Binary
dedup_cols_l2 = ['DOI_Reference', 'NP_Name', 'Cell_Lines', 'Toxicity_Binary']
l2_dupes = df_with_doi.duplicated(subset=dedup_cols_l2, keep=False)
l2_groups = df_with_doi[l2_dupes].groupby(dedup_cols_l2).size()

print(f"\n=== Level 2: Same DOI + NP_Name + Cell_Lines + Toxicity_Binary ===")
print(f"Duplicate rows: {l2_dupes.sum()}")
print(f"Duplicate groups: {len(l2_groups)}")

# Show some example groups
print(f"\n=== Sample duplicate groups (Level 1) ===")
for i, (key, group_df) in enumerate(df_with_doi[l1_dupes].groupby(dedup_cols_l1)):
    if i >= 5:
        break
    print(f"\nGroup {i+1}: DOI={key[0]}, NP={key[1]}, Cell={key[2]}")
    cols_to_show = ['Record_ID', 'Primary_Size_nm', 'Dose_InVitro_Max_ugmL', 
                    'Cell_Viability_pct', 'Toxicity_Binary', 'Source']
    print(group_df[cols_to_show].to_string(index=False))

# %%
# Cell 14: Cross-source duplicate check
# Are the same papers appearing in both Mumbai and Himadri?
cross_source = (
    df_with_doi
    .groupby('DOI_Reference')['Source']
    .apply(lambda x: set(x))
)
multi_source_dois = cross_source[cross_source.apply(len) > 1]

print(f"=== Cross-Source DOI Analysis ===")
print(f"Total unique DOIs: {len(cross_source)}")
print(f"DOIs appearing in multiple sources: {len(multi_source_dois)}")

if len(multi_source_dois) > 0:
    # Count rows affected
    multi_doi_list = multi_source_dois.index.tolist()
    affected_rows = df_with_doi[df_with_doi['DOI_Reference'].isin(multi_doi_list)]
    print(f"Rows affected: {len(affected_rows)}")
    print(f"\nTop 10 cross-source DOIs:")
    for doi in multi_doi_list[:10]:
        sources = multi_source_dois[doi]
        count = len(affected_rows[affected_rows['DOI_Reference'] == doi])
        print(f"  {doi} -> Sources: {sources}, Rows: {count}")
else:
    print("No cross-source overlap found — each DOI appears in only one source.")

print("\n[NOTE] Dedup action is PENDING sir's decision.")
print("       No rows removed in this step.")
print("\n[DONE] Step 7 complete (analysis only)")

# %% [markdown]
# ---
# ## Step 8: Drop Cell_Viability_pct (Q4)
# 
# Yash confirmed: Cell viability will NOT be a user input feature at prediction time. Drop as feature column.
# 
# Also dropping `Label_Viability_Flag` since it was derived from Cell_Viability_pct.

# %%
# Cell 15: Drop Cell_Viability_pct and Label_Viability_Flag
cols_to_drop = ['Cell_Viability_pct', 'Label_Viability_Flag']

print(f"Before drop: {df.shape[1]} columns")
for col in cols_to_drop:
    if col in df.columns:
        print(f"  Dropping '{col}' — non-null: {df[col].notna().sum()}/{len(df)}")

df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

print(f"After drop: {df.shape[1]} columns")
print("\n[DONE] Step 8 complete")

# %% [markdown]
# ---
# ## Step 9: Flag Rows Pending Expert Input (Q2, Q3)
# 
# NO CHANGES made here — just flagging rows for later action.
# - Q2: 55 rows with low viability + Non-toxic label (pending sir)
# - Q3: 191 rows with missing Toxicity_Binary (pending sir)

# %%
# Cell 16: Flag pending rows — NO CHANGES, analysis only

# Q3: Missing Toxicity_Binary
missing_target = df['Toxicity_Binary'].isna()
print(f"=== Q3: Missing Toxicity_Binary ===")
print(f"Rows with no toxicity label: {missing_target.sum()}")
print(f"Source breakdown:")
print(df[missing_target]['Source'].value_counts().to_string())
print(f"NP_Type breakdown:")
print(df[missing_target]['NP_Type'].value_counts(dropna=False).to_string())

print(f"\n=== Q2: Low-Viability Non-toxic (from original data, pre-drop) ===")
print(f"These 55 rows were identified in the conflict analysis.")
print(f"Since Cell_Viability_pct has been dropped, we track them by Record_ID.")
print(f"Reload original dataset to get the list if needed for sir's review.")

print(f"\n[NOTE] No changes applied. Pending expert consultation.")
print("\n[DONE] Step 9 complete")

# %% [markdown]
# ---
# ## Step 10: Drop DOI_Reference + Final Summary & Save
# 
# DOI was kept through Steps 7 for dedup analysis. Now drop it.
# Then save the cleaned dataset and print a final summary.

# %%
# Cell 17: Drop DOI_Reference
if 'DOI_Reference' in df.columns:
    print(f"Dropping DOI_Reference ({df['DOI_Reference'].notna().sum()} non-null values)")
    df.drop(columns=['DOI_Reference'], inplace=True)

print(f"Final column count: {df.shape[1]}")

# %%
# Cell 18: Final summary
print("=" * 60)
print("  PREPROCESSING SUMMARY")
print("=" * 60)
print(f"\nDataset shape: {df.shape[0]} rows x {df.shape[1]} columns")
print(f"\n--- Columns retained ({df.shape[1]}) ---")
for i, col in enumerate(df.columns, 1):
    dtype = df[col].dtype
    non_null = df[col].notna().sum()
    pct = non_null / len(df) * 100
    print(f"  {i:2d}. {col:<35s} {str(dtype):<10s} {non_null:,}/{len(df):,} ({pct:.1f}%)")

print(f"\n--- Changes Applied ---")
print(f"  [DONE] Step 1: Case standardization (Toxicity_Binary, Label_Original, NP_Subtype)")
print(f"  [DONE] Step 2: Dropped Source_ID (100% empty)")
print(f"  [DONE] Step 3: Fixed negative dosage (2 rows -> NaN)")
print(f"  [DONE] Step 4: Remapped mislabeled Toxic -> Non-toxic")
print(f"  [DONE] Step 5: Inferred NP_Type from Material_Category")
print(f"  [DONE] Step 6: Hydrodynamic size > 1000nm -> NaN (tentative)")
print(f"  [WAIT] Step 6: Zeta outliers flagged, not changed")
print(f"  [WAIT] Step 7: Near-duplicates analyzed, no rows removed")
print(f"  [DONE] Step 8: Dropped Cell_Viability_pct, Label_Viability_Flag")
print(f"  [WAIT] Step 9: Q2 (55 rows) and Q3 (191 rows) pending sir")
print(f"  [DONE] Step 10: Dropped DOI_Reference")

print(f"\n--- Toxicity_Binary Distribution ---")
print(df['Toxicity_Binary'].value_counts(dropna=False).to_string())

print(f"\n--- NP_Type Distribution ---")
print(df['NP_Type'].value_counts(dropna=False).to_string())

print(f"\n--- Missing Values (top 15) ---")
missing = df.isnull().sum().sort_values(ascending=False)
missing_pct = (missing / len(df) * 100).round(1)
for col in missing.head(15).index:
    if missing[col] > 0:
        print(f"  {col:<35s} {missing[col]:>5d} ({missing_pct[col]}%)")

# %%
# Cell 19: Save preprocessed dataset
output_path = DATA_DIR / 'version 2 dataset.xlsx'
df.to_excel(output_path, index=False, engine='openpyxl')

print(f"Saved to: {output_path}")
print(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
print(f"\nOriginal backup at: {DATA_DIR / 'version 2 dataset - original.xlsx'}")
print("\n[DONE] Preprocessing pipeline complete.")


