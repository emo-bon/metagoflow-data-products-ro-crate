#! /usr/bin/env python3

"""Script to build a sample table for the Github repository README file."""

from pathlib import Path
import pandas as pd

OBSERVATORY_LOGSHEETS_PATH = (
    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
    "refs/heads/main/validated-data/Observatory_combined_logsheets_validated.csv"
)

COMBINED_LOGSHEETS_PATH = (
    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
    "refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv"
)


def get_existing_rocrates(path_to_cluster):
    """Return a list of existing ro-crates"""
    utils_path = Path(__file__).resolve()
    utils_dir = utils_path.parent
    path_to_rocrates = Path(utils_dir, path_to_cluster)
    existing_rocrates_paths = list(Path(path_to_rocrates).glob("*-ro-crate"))
    existing_rocrates_names = [p.name for p in existing_rocrates_paths]
    print(f"Found {len(existing_rocrates_names)} rocrates")
    return existing_rocrates_names


obs_logsheet = pd.read_csv(
    OBSERVATORY_LOGSHEETS_PATH, encoding="utf-8", on_bad_lines="warn"
)
data_sheet = pd.read_csv(COMBINED_LOGSHEETS_PATH, encoding="utf-8", on_bad_lines="warn")
rocrates = get_existing_rocrates("../analysis-results-cluster-01-crate")
lines = []

for rocrate in rocrates:
    # EMOBON_VB_Wa_96-ro-crate
    rocrate_name = rocrate.split("-ro-crate")[0]
    obs_id, stype = rocrate.split("_")[1:3]
    if stype == "So":
        stype = "soft sediment"
    elif stype == "Wa":
        stype = "water"
    else:
        print(f"Error unknown sample type {stype}")
    row_obs = obs_logsheet.loc[(obs_logsheet["obs_id"] == obs_id)].to_dict()
    obs_name = list(row_obs["loc_loc"].values())[0]
    obs_country = list(row_obs["geo_loc_name"].values())[0]
    lat = list(row_obs["latitude"].values())[0]
    long = list(row_obs["longitude"].values())[0]
    loc_link = f"https://www.google.com/maps/search/?api=1&query={lat},{long}"
    obs_location = f"[{obs_name}]({loc_link})"

    row_data = data_sheet.loc[(data_sheet["source_mat_id"] == rocrate_name)].to_dict()
    try:
        date = list(row_data["samp_store_date"].values())[0]
    except IndexError:
        print(f"Failed on {rocrate_name} and {rocrate}")
    lines.append(
        f"| {rocrate_name} | {obs_location} | {obs_country} | {stype} | {date} |"
    )

lines.sort()
print("| RO-Crate name | Observatory location | Country | Sample type | Sampling date|")
for line in lines:
    print(line)
