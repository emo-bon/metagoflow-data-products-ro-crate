#! /usr/bin/env python3

import pandas as pd

# The combined sampling event logsheets for batch 1 and 2
COMBINED_LOGSHEETS_PATH = (
    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
    "refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv"
)

df = pd.read_csv(COMBINED_LOGSHEETS_PATH)
print(f"df.shape={df.shape}")
wc = df[df["env_package"] == "water_column"]
ss = df[df["env_package"] == "soft_sediment"]
print(f"wc.shape={wc.shape}")
print(f"ss.shape={ss.shape}")
named_fields = ["sampling_event", "collection_date", "size_frac", "replicate"]
sampled_wc = wc[named_fields]
print(f"sampled_wc={sampled_wc.shape}")
print(sampled_wc)
unique_rows = sampled_wc[sampled_wc.duplicated(subset=["sampling_event",
    "collection_date", "size_frac"], keep="first")]
print(unique_rows.shape)
#print(unique_rows)

for i, row in unique_rows.iterrows():
    print
    print(f"sampling_event = {row['sampling_event']}")
    print(f"collection_date = {row['collection_date']}")
    print(f"size_frac = {row['size_frac']}")
    selected_rows = sampled_wc[
        (sampled_wc['sampling_event'] == row['sampling_event']) &
        (sampled_wc['collection_date'] == row['collection_date']) &
        (sampled_wc['size_frac'] == row['size_frac'])
        ]
    #Filter out 'blank_1'
    selected_rows = selected_rows[selected_rows['replicate'] != 'blank_1']
    print(f"selected_rows.shape={selected_rows.shape}")
    #print(selected_rows)
    reps = selected_rows["replicate"].tolist()
    reps.sort()
    for i, r in enumerate(reps):
        if str(i+1) != r:
            print(f"Error: replicates out of order: {reps} {str(i+1)} != {r}")
            print(selected_rows)
            break
            

