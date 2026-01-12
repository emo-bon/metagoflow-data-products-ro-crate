#! /usr/bin/env python3

"""
Identify biological replicates among the EM BON metagenomes.

In the observatory logsheets:
e.g. BPNS: https://docs.google.com/spreadsheets/d/1mEi4Bd2YR63WD0j54FQ6QkzcUw_As9Wilue9kaXO2DE

The main identfier - source_mat_id - is incremented for every row of the sheet.
Biological replicates are indicated in the "replicate" column.

What we want is to identify replicates by their source_mat_ids
e.g.
EMOBON_AAOT_Wa_1 and EMOBON_AAOT_Wa_2 are biological replicates

Do from biological_replicates import REPLICATES

"""

import pandas as pd

# The combined sampling event logsheets for batch 1 and 2
COMBINED_LOGSHEETS_PATH = (
    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
    "refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv"
)

# Extract the wc and ss samples for the combined sheet
df = pd.read_csv(COMBINED_LOGSHEETS_PATH)
#wc = df[df["env_package"] == "water_column"]
#ss = df[df["env_package"] == "soft_sediment"]

duplicates_mask = df.duplicated(subset=["sampling_event", "collection_date", "size_frac"], keep=False)
all_duplicates = df[duplicates_mask]

#Filter out 'blank_1'
duplicates = all_duplicates[all_duplicates['replicate'] != 'blank_1']

groups = duplicates.groupby(
        ["sampling_event", "collection_date", "size_frac"]
        )

def iterate_replicates(groups):
    """Generator that yields pairs of biological repliates"""
    for _, group in groups:
        yield [row['source_mat_id'] for index, row in group.iterrows()]

REPLICATES = iterate_replicates(groups)

