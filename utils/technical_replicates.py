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

WATER_COLUMN_REPLICATES and SOFT_SEDIMENT_REPLICATES are generators for
technical replicate pairs.


"""

import sys
import math
import subprocess
import logging as log
import pandas as pd
from pathlib import Path

# The combined sampling event logsheets for batch 1 and 2
COMBINED_LOGSHEETS_PATH = (
    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
    "refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-11-12.csv"
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

def get_technical_replicates():
    # Extract the wc and ss samples for the combined sheet
    df = pd.read_csv(COMBINED_LOGSHEETS_PATH)
    wc_samples = df[df["env_package"] == "water_column"]
    ss_samples = df[df["env_package"] == "soft_sediment"]
    wc_groups = _find_wc_grouped_replicates(wc_samples)
    ss_groups = _find_ss_grouped_replicates(ss_samples)

    #WATER_COLUMN_REPLICATES = iterate_replicates(wc_groups)
    #SOFT_SEDIMENT_REPLICATES = iterate_replicates(ss_groups)

    return [_iterate_replicates(wc_groups), _iterate_replicates(ss_groups)]

WATER_COLUMN_REPLICATES,SOFT_SEDIMENT_REPLICATES = get_technical_replicates()

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
                    break
            elif math.isnan(row[0]):
                # Not all samples with an EMO BON code were sent to sequencing
                continue
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
    """

    outpath.mkdir(exist_ok=True)

    local_paths = []
    for path in paths:
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
    

def download_raw_sequences_of_replicate_pair(pair, outpath="data"):
    """
    Download the raw sequence data for a pair of technical replicates
    pair is a list of the two source_mat_id's
    """
    pp = []
    for source_mat_id in pair:
        paths = _get_raw_sequence_file_names(source_mat_id)
        lps = _download_data_files(paths, Path(outpath, source_mat_id))
        for lp in lps:
            log.debug(f"Local file: {lp}")
        pp.append(lps)
    return pp

def main(debug=False):
    """ Run test using first technical pair in SEDIMENTS
    """    

    # Logging
    if debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO
    log.basicConfig(format="\t%(levelname)s: %(message)s", level=log_level)

    first_pair = list(SOFT_SEDIMENT_REPLICATES)[0]
    log.info(
        f"Running test on first pair of SEDIMENT technical replicates"
        f"{first_pair}"
        )
    data_paths = download_raw_sequences_of_replicate_pair(
        first_pair,
        outpath = "raw_sequence_data"
        )
    log.info("Local paths:")
    for path in data_paths:
        log.info(f"\t {path}")

if __name__ == "__main__":
    main()
