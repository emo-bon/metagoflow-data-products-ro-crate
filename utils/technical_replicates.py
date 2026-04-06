#! /usr/bin/env python3

"""
Identify technical replicates among the EM BON metagenomes. Download raw
sequence data files for a technical pair.

In the observatory logsheets:
e.g. BPNS: https://docs.google.com/spreadsheets/d/1mEi4Bd2YR63WD0j54FQ6QkzcUw_As9Wilue9kaXO2DE

The main identfier - source_mat_id - is incremented for every row of the sheet.
Technical replicates are indicated in the "replicate" column.

What we want is to identify replicates by their source_mat_ids
e.g. EMOBON_AAOT_Wa_1 and EMOBON_AAOT_Wa_2 are technical replicates

These are broken pairs and should not be combined:
BPNS_So_5 and BPNS_So_6
BPNS_So_17 and BPNS_So_18
RFormosa_So_1 and RFormosa_So_2
ROSKOGO_So_16 and ROSKOGO_So_17
"""

import sys
import math
import urllib
import subprocess
import logging as log
import pandas as pd
from pathlib import Path

# The combined sampling event logsheets for batch 1 and 2
#COMBINED_LOGSHEETS_PATH = (
#    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
#    "refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv"
#)

# 
OBSERVATORIES_LOGSHEET = (
    "https://raw.githubusercontent.com/emo-bon/governance-crate/"
    "refs/heads/main/observatories.csv"
)
# The run-information files for each batch set to sequencing facility
BATCH1_RUN_INFO_PATH = (
    "https://raw.githubusercontent.com/emo-bon/sequencing-logistics-crate/main/"
    "shipment/batch-001/run-information-batch-001.csv"
)
BATCH2_RUN_INFO_PATH = (
    "https://raw.githubusercontent.com/emo-bon/sequencing-logistics-crate/main/"
    "shipment/batch-002/run-information-batch-002.csv"
)
BATCH3_RUN_INFO_PATH = (
    "https://raw.githubusercontent.com/emo-bon/sequencing-logistics-crate/main/"
    "shipment/batch-003-0/run-information-batch-003.csv"
)
# Path to sequence data archive
DATA_ARCHIVE = "ceta-storage:/mnt/storage-data-pools/emo-bon-sequencing-data"

BROKEN_REPLICATE_PAIRS = [
    ("EMOBON_BPNS_So_5", "EMOBON_BPNS_So_6"),
    ("EMOBON_BPNS_So_17", "EMOBON_BPNS_So_18"),
    ("EMOBON_RFormosa_So_1", "EMOBON_RFormosa_So_2"),
    ("EMOBON_ROSKOGO_So_16", "EMOBON_ROSKOGO_So_17")
]

def _read_observatory_names():
    """
    """
    df = pd.read_csv(OBSERVATORIES_LOGSHEET, encoding='iso-8859-1')
    all_stations = df[["EMOBON_observatory_id"]].values.tolist()
    stations = [station for sublist in all_stations for station in sublist]
    log.debug(f"Stations: {stations}")
    return stations

def _find_wc_grouped_replicates(samples):
    duplicates_mask = samples.duplicated(
        subset=["sampling_event", "collection_date", "size_frac"],
        keep=False
        )
    all_duplicates = samples[duplicates_mask]

    #Filter out 'blank_1'
    duplicates = all_duplicates[all_duplicates['replicate'] != 'blank_1']

    groups = duplicates.groupby(
        ["sampling_event", "collection_date", "size_frac"]
        )
    return groups

def _find_ss_grouped_replicates(samples):
    duplicates_mask = samples.duplicated(
        subset=["sampling_event", "collection_date"], # No size_frac
        keep=False
        )
    all_duplicates = samples[duplicates_mask]

    #Filter out 'blank_1'
    duplicates = all_duplicates[all_duplicates['replicate'] != 'blank_1']

    groups = duplicates.groupby(
        ["sampling_event", "collection_date"]
        )
    return groups

def _iterate_replicates(groups):
    """Generator that yields pairs of technical repliates"""
    for _, group in groups:
        yield [row['source_mat_id'] for index, row in group.iterrows()]

def get_technical_replicates(observatory_name, env_package):
    # Extract the wc and ss samples for a named observatory
    if not env_package in ["filters", "sediments"]:
        log.error(f"env_package must be either 'filters' or 'sediments'")
        sys.exit()
    sheet_type = "sediment" if env_package == "sediments" else "water"
    observatory_sheet = (
        f"https://raw.githubusercontent.com/emo-bon/"
        f"observatory-{observatory_name.lower()}-crate/"
        f"refs/heads/main/logsheets/transformed/{sheet_type}_sampling.csv"
    )
    log.debug(f"obs_sheet = {observatory_sheet}")
    if env_package == "sediments":
        sheet_env_name = "soft_sediment"
    else:
        sheet_env_name = "water_column"
    try:
        samples = pd.read_csv(observatory_sheet)
    except urllib.error.HTTPError:
        log.info(f"{observatory_name} {env_package} : missing - {observatory_sheet}")
        return None
    #samples = df.dropna(how='all')
    if env_package == "sediments":
        groups = _find_ss_grouped_replicates(samples)
    else:
        groups = _find_wc_grouped_replicates(samples)

    return _iterate_replicates(groups)

def _get_raw_sequence_file_names(source_mat_id):
    """
    Given a source_mat_id use the shipping sheet to identify the sequence file
    and path

    Sheets:
    run-information-batch-001.csv
    run-information-batch-002.csv
    run-information-batch-003.csv

    Columns
    source_mat_id - EMOBON_OOB_So_1 - {So}/{Wa} - {SEDIMENT}/{FILTERS}
    reads_name - DBH_AAAAOSDA_1_HWLTKDRXY.UDI235
    run - 220223_JARVIS_HWLTKDRXY

    full_reads_name = DBH_AAAAOSDA_1_{1,2}_HWLTKDRXY.UDI235_clean.fastq.qz
    
    Path:
    ""
    "www.genoscope.cns.fr/sadc/projet_DBB/{FILTERS}/{run}/{full_reads_name}
    
    """
    log.info(f"Preparing: {source_mat_id}")
    env_package = source_mat_id.split("_")[2]
    if env_package not in ["Wa", "So"]:
        log.error("Cannot identify env_package from source_mat_id")
    if env_package == "Wa":
        env_package_upper = "FILTERS"
    elif env_package == "So":
        env_package_upper = "SEDIMENTS"
    else:
        log.error("Cannot identify env_package {env_package}")
        sys.exit()

    found = False
    for batch in [BATCH1_RUN_INFO_PATH, BATCH2_RUN_INFO_PATH, BATCH3_RUN_INFO_PATH]:
        df = pd.read_csv(batch, encoding='iso-8859-1')
        for row in df[["source_mat_id", "run", "reads_name"]].values.tolist():
            if isinstance(row[0], str):
                # print(row)
                if row[0] == source_mat_id:
                    run = row[1]
                    reads_name = row[2]
                    log.info(f"Found run: {run}")
                    log.info(f"Found reads_name: {reads_name}")
                    found = True
                    break
            elif math.isnan(row[0]):
                # Not all samples with an EMO BON code were sent to sequencing
                continue
        if found:
            break
    else:
        log.error(f"Cannot find {source_mat_id}")
        sys.exit()

    filenames = []
    for n in ["1", "2"]:
        bits = reads_name.rsplit("_", 1)
        bits.insert(1, n)
        bits.append("clean.fastq.gz")
        filenames.append("_".join(bits))
    
    return [Path("www.genoscope.cns.fr/sadc/projet_DBB", 
        env_package_upper, run, fname) for fname in filenames]


def _download_data_files(paths, outpath):
    """
    SCP in a subprocess seems easiest
    
    outdir is a Path() to a directory named for the source_mat_id
    """
    # Check to see if the raw seq files are already present in the source_mat_id
    # directory
    already_exists = False
    try:
        outpath.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        log.info(f"Directory '{outpath}' already exists.")
        already_exists = True

    local_paths = []
    for path in paths:
        if not already_exists:
            p = Path(DATA_ARCHIVE, path)
            scp_command = ["scp", "-q", p, outpath]
            log.debug(f"scp command: {scp_command}")
            log.info(f"Downloading: {p.name}")
            try:
                subprocess.run(scp_command, check=True)
                log.debug(f"File downloaded successfully to {outpath}")
            except subprocess.CalledProcessError as e:
                log.error(f"Error downloading file: {e}")
        local_paths.append(Path(outpath, path.name))
    return local_paths
    

def download_raw_sequences_of_replicate_pair(pair, outpath):
    """
    Download the raw sequence data for a pair of technical replicates
    pair is a list of the two source_mat_id's

    outpath is a top_level data directory e.g. "raw_sequence_data" inside which
    each item of a techincal replciate will have a dir named by the source_mat_id
    """
    pp = []
    for source_mat_id in pair:
        paths = _get_raw_sequence_file_names(source_mat_id)
        lps = _download_data_files(paths, Path(outpath, source_mat_id))
        for lp in lps:
            log.debug(f"Local file: {lp}")
        pp.append(lps)
    return pp

def main(test_download=False, debug=False):
    """ Run test using first technical pair in SEDIMENTS
    """    

    # Logging
    if debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO
    log.basicConfig(format="\t%(levelname)s: %(message)s", level=log_level)

    observatory_abbreviated_names = _read_observatory_names()
    for obs_name in observatory_abbreviated_names:
        for env_package in ["filters", "sediments"]:
            replicate_pairs = []
            replicate_pairs_generator = get_technical_replicates(obs_name, env_package)
            if replicate_pairs_generator:
                replicate_pairs = list(get_technical_replicates(obs_name, env_package))
            lognote = f"Station {obs_name} {env_package}"
            log.info(f"{lognote} {len(replicate_pairs)}")
            count = 1
            for replicate_pair in replicate_pairs:
                log.info(f"\t\t{lognote} {count}: {replicate_pair}")
                count += 1

if __name__ == "__main__":
    main()

