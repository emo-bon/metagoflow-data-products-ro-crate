#! /usr/bin/env python3

import os
import logging as log
from pathlib import Path
import pandas as pd
from utils import get_refcode_and_source_mat_id_from_run_id

"""
Check the MGF analyses in the run tracking sheet against the RO-Crates in the
analysis-results-cluster-0[1,2]-crate repositories
"""

# These are in create-ro-crate.py
# The MGF _Run_Track Google Sheets
FILTERS_MGF_PATH = (
    "https://docs.google.com/spreadsheets/d/"
    "1j9tRRsRCcyViDMTB1X7lx8POY1P5bV7UijxKKSebZAM/gviz/tq?tqx=out:csv&sheet=FILTERS"
)
SEDIMENTS_MGF_PATH = (
    "https://docs.google.com/spreadsheets/d/"
    "1j9tRRsRCcyViDMTB1X7lx8POY1P5bV7UijxKKSebZAM/gviz/tq?tqx=out:csv&sheet=SEDIMENTS"
)
# Ouch needed for relative imports in other scripts
ROCRATE_REPO_NAME = "analysis-results-cluster-01-crate"
abs_path_of_current_script = Path(__file__).resolve().parent.parent
def path_to_rocrate_repo(base_path):
    return Path(base_path, ROCRATE_REPO_NAME)
ROCRATE_REPO = path_to_rocrate_repo(abs_path_of_current_script)

def parse_sheet(sheet, repo_path=ROCRATE_REPO, debug=False):
    """
    Given an env_package name, e.g. either "sediments" or "filters",
    return a tuple of dictionaries for "found" or "missing" ro-crates with
    respect to the sequenced samples (have a ref_code) in the MetaGOflow Run Sheets

    Found: ro-crate exists for that sequenced sample
    Missing: ro-crate does not exist for that sequenced sample
    
    Return: (
            # Found
            {station: ((source_mat_id, ref_code, run_id), etc...),
            etc...
            },
            # Missing
            {station: ((source_mat_id, ref_code, run_id), etc...),
             etc...
            }
            )
    """

    
    if sheet == "filters":
        sheet_path = FILTERS_MGF_PATH
        # DBB_AAAOOSDA_4_1_HMGW5DSX3.UDI226
        name_index = -1
        abbrev = "Wa"
    if sheet == "sediments":
        sheet_path = SEDIMENTS_MGF_PATH
        # DBH_AAAAOSDA_1_1_HWLTKDRXY.UDI235_clean.fastq.gz
        name_index = -2
        abbrev = "So"

    # Because HCMR has another - in it
    # >>> s.rsplit("-", 2) = ['EMOBON_HCMR-1_Wa_5', 'ro', 'crate']
    all_ro_crate_names = [
        d.name.rsplit("-", 2)[0] for d in Path(ROCRATE_REPO).iterdir()
        if d.is_dir() and not d.name.startswith(".")
    ]
    ro_crate_names = [n for n in all_ro_crate_names if abbrev in n]
    log.debug(f"Found {len(ro_crate_names)} ro-crates in {ROCRATE_REPO}")
    for d in ro_crate_names:
        log.debug(d)
        
    log.info(f"Doing {sheet}")
    data = pd.read_csv(sheet_path, encoding='iso-8859-1')
    
    # Check batch numbers
    count = 0
    for row in data[["Batch Number", "ref_code", "Forward Read Filename"]].values.tolist():
        if row[0] in [1.0, 2.0]:
            log.debug(f"{row[0]} : {row[1]} = {row[2]}")
            count += 1
    log.debug(f"Final count: {count} in {sheet}")

    # Check source_mat_id
    missing = []
    found = []
    for row in data[["Batch Number", "ref_code", "Forward Read Filename"]].values.tolist():
        if row[0] in [1.0, 2.0]:
            run_id = row[2].split("_")[name_index]
            ref_code, source_mat_id = get_refcode_and_source_mat_id_from_run_id(run_id)
            log.debug(f"batch {row[0]} : run_id {run_id} : ref_code {ref_code} : {source_mat_id}")
            if source_mat_id in ro_crate_names:
                found.append((source_mat_id, ref_code, run_id))
                log.debug(f"{source_mat_id} found")
                log.debug(f"Removing {source_mat_id}")
                ro_crate_names.remove(source_mat_id)
            else:
                log.debug(f"{source_mat_id} missing")
                missing.append((source_mat_id, ref_code, run_id))

    log.info(f"Missing {len(missing)}")
    for n in missing:
        log.debug(f"{n[0]}\t{n[1]}\t{n[2]}")
    log.debug(f"Found {len(found)}")
    for n in found:
        log.debug(f"{n[0]}\t{n[1]}\t{n[2]}")

    foundd = {}
    for n in found:
        station = n[0].split('_')[1]
        foundd[station] = foundd.get(station, []) + [(n[0], n[1], n[2])]
    missingd = {}
    for n in missing:
        station = n[0].split('_')[1]
        missingd[station] = missingd.get(station, []) + [(n[0], n[1], n[2])]
    for station in foundd:
        log.debug(f"Found {station}")
        for record in foundd[station]:
            log.debug(f"{record[0]}\t {record[1]}\t {record[2]}")
    for station in missingd:
        log.debug(f"Missing {station}")
        for record in missingd[station]:
            log.debug(f"{record[0]}\t {record[1]}\t {record[2]}")
    if len(ro_crate_names) == 0:
        log.info("All ro-crates accounted for")
    else:
        for ro_crate in ro_crate_names:
            log.info(f"RO-crate present but sample not found: {ro_crate}")
    return foundd, missingd,


def main(debug=False):
    """
    There are 151 FILTER samples in Batch 1 and 2 combined
    There are 30 SEDIMENT samples in Batch 1 and 2 combined
    """
    # Logging
    if debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO
    log.basicConfig(format="\t%(levelname)s: %(message)s", level=log_level)
   
    for sheet in ["filters", "sediments"]: 
        parse_sheet(sheet)
    

if __name__ == "__main__":
    main()

