import math
import os
import sys
import subprocess
import logging
from pathlib import Path
import shutil
import pandas as pd

log = logging.getLogger(__name__)

"""
Utilities for dealing with MGF data archives


"""


def find_bzip2():
    # Find best bzip2 programme
    if shutil.which("lbzip2"):
        # Get nunmber of threads/cpu cores
        bzip2_program = "lbzip2"
    elif shutil.which("bzip2"):
        log.info("Using bzip2")
        bzip2_program = "bzip2"
    else:
        log.error("Cannot find lbzip2 or bzip2")
        log.error("Exiting...")
        sys.exit()
    return bzip2_program


def open_archive(tarball_file, bzip2_program):
    """
    You are expected to be in the dir with the tarball when calling this function
    """
    # Create temp dir in which to untar archives
    try:
        Path("temp").mkdir(exist_ok=False)
    except FileExistsError as e:
        log.error(
            f"A directory called 'temp' in the target directory already exists: {e}"
        )
        log.error("Exiting...")
        sys.exit()
    os.chdir("temp")
    log.debug(f"Opening archive {tarball_file} with {bzip2_program}")
    # tar --use-compress-program lbunzip2 -xvf ../HMNJKDSX3.UDI200.tar.bz2
    subprocess.check_call(
        [
            "tar",
            "--use-compress-program",
            f"{bzip2_program}",
            "-xf",
            f"{tarball_file}",
        ]
    )
    # Check archive has a top-level directory called run_id
    run_id = Path(str(tarball_file.name).rsplit(".", 2)[0])
    log.debug(f"run_id = {run_id}")
    if run_id.exists():
        os.chdir("..")
        # Move archive up to target directory
        Path("temp", run_id).rename(run_id)
        shutil.rmtree("temp")
    else:
        # Archive with top level directory
        log.debug("Checking archive in ./temp")
        yml_files = list(Path.cwd().glob("*.yml"))
        log.debug(f"Found {yml_files} yml files")
        if Path("results").exists() and len(yml_files) == 2:
            log.debug("Found archive without top level directory")
            os.chdir("..")
            log.debug(f"Renaming ./temp to {run_id}")
            Path("temp").rename(run_id)
        else:
            # Deal with broken archive
            log.error(f"Archive looks completely broken at {tarball_file}")
            os.chdir("..")
            shutil.rmtree("temp")
            log.debug("Renaming broken archive")
            Path(tarball_file).rename(f"{tarball_file}-broken")


# Legacy code dont use; just open_archive
def fix_all_archives(target_directory, fix_archive, debug):
    """MGF archives should have a top level directory with the run_id
    e.g. HMNJKDSX3.UDI20 below which sits the results directory

    """

    log.basicConfig(
        format="\t%(levelname)s: %(message)s", level=log.DEBUG if debug else log.INFO
    )

    # Check the target_directory name
    target_directory = Path(target_directory)
    if not target_directory.exists():
        log.error(f"Cannot find the target directory {target_directory}")
        sys.exit()
    log.debug(f"Found target directory {target_directory}")

    # CD to target directory
    log.info(f"Changing directory to {target_directory}")
    home_dir = Path.cwd()
    os.chdir(target_directory)

    # Get list of tarball files
    tarball_files = list(Path.cwd().glob("*.tar.bz2"))

    log.debug(f"Found {len(tarball_files)} tarball files")
    for tarball in tarball_files:
        log.debug(f"Tarball: {tarball}")
        log.debug(f"Tarball name: {tarball.name}")
    tarball_files = [f.name for f in tarball_files]
    if len(tarball_files) == 0:
        log.error(f"Cannot find any tarball files in {target_directory}")
        sys.exit()
    else:
        # Where the open archives will go
        log.debug("Creating fixed-archives directory")
        Path("fixed-archives").mkdir(exist_ok=True)

    bzip2_program = find_bzip2()

    for tarball_file in tarball_files:
        run_id = Path(str(tarball_file).rsplit(".", 2)[0])
        log.debug(f"run_id = {run_id}")
        open_archive(tarball_file, bzip2_program)

    os.chdir(home_dir)


def get_refcode_and_source_mat_id_from_run_id(run_id):
    """
    Extract the EMO BON ref_code and source_mat_id from the run_id.

    ref_code is assigned to the sample when it is sent for seqeuencing.
    e.g. EMOBON00084

    run_id is the last part of the reads_name in the run information file
    e.g. HWLTKDRXY.UDI235

    """

    assert isinstance(run_id, str), "run_id must be a string"

    # "https://raw.githubusercontent.com/emo-bon/sequencing-data/main/shipment/"
    # "batch-001/run-information-batch-001.csv"
    BATCH1_RUN_INFO_PATH = (
        "https://raw.githubusercontent.com/emo-bon/sequencing-crate/refs/heads/main/shipment/"
        "batch-001/run-information-batch-001.csv"
    )
    # "https://raw.githubusercontent.com/emo-bon/sequencing-data/main/shipment/"
    # "batch-002/run-information-batch-002.csv"
    BATCH2_RUN_INFO_PATH = (
        "https://raw.githubusercontent.com/emo-bon/sequencing-crate/refs/heads/main/shipment/"
        "batch-002/run-information-batch-002.csv"
    )
    BATCH3_RUN_INFO_PATH = (
        "https://raw.githubusercontent.com/emo-bon/sequencing-logistics-crate/refs/heads/main/"
        "shipment/batch-003-0/run-information-batch-003.csv"
    )

    # for i, batch in enumerate([BATCH1_RUN_INFO_PATH, BATCH2_RUN_INFO_PATH, BATCH3_RUN_INFO_PATH]):
    for batch in [BATCH1_RUN_INFO_PATH, BATCH2_RUN_INFO_PATH, BATCH3_RUN_INFO_PATH]:
        print(f"Reading {batch}")
        df = pd.read_csv(batch, encoding='iso-8859-1')
        for row in df[["reads_name", "ref_code", "source_mat_id"]].values.tolist():
            if isinstance(row[0], str):
                id_in_row = str(row[0].split("_")[-1])
                if id_in_row == run_id:
                    return (row[1], row[2])
            elif math.isnan(row[0]):
                # Not all samples with an EMO BON code were sent to sequencing
                continue
    else:
        print(f"Cannot find run_id {run_id} in any of the run information files")
        return (None, None)


def get_run_id_and_ref_code_from_source_mat_id(source_mat_id):
    """
    Extract the EMO BON reads_name and ref_code from the source_mat_id.

    ref_code is assigned to the sample when it is sent for seqeuencing.
    e.g. EMOBON00084

    run_id is the last part of the reads_name in the run information file
    e.g. HWLTKDRXY.UDI235

    source_mat_id is the EMO BON code assigned to the sample
    e.g. EMOBON_NRMCB_Wa_44

    """

    # "https://raw.githubusercontent.com/emo-bon/sequencing-data/main/shipment/"
    # "batch-001/run-information-batch-001.csv"
    BATCH1_RUN_INFO_PATH = (
        "https://raw.githubusercontent.com/emo-bon/sequencing-crate/refs/heads/main/shipment/"
        "batch-001/run-information-batch-001.csv"
    )
    # "https://raw.githubusercontent.com/emo-bon/sequencing-data/main/shipment/"
    # "batch-002/run-information-batch-002.csv"
    BATCH2_RUN_INFO_PATH = (
        "https://raw.githubusercontent.com/emo-bon/sequencing-crate/refs/heads/main/shipment/"
        "batch-002/run-information-batch-002.csv"
    )
    BATCH3_RUN_INFO_PATH = (
        "https://raw.githubusercontent.com/emo-bon/sequencing-logistics-crate/refs/heads/main/"
        "shipment/batch-003-0/run-information-batch-003.csv"
    )

    # for i, batch in enumerate([BATCH1_RUN_INFO_PATH, BATCH2_RUN_INFO_PATH, BATCH3_RUN_INFO_PATH]):
    for batch in [BATCH1_RUN_INFO_PATH, BATCH2_RUN_INFO_PATH, BATCH3_RUN_INFO_PATH]:
        #print(f"Reading {batch}")
        df = pd.read_csv(batch, encoding='iso-8859-1')
        for row in df[["reads_name", "ref_code", "source_mat_id"]].values.tolist():
            if isinstance(row[0], str):
                # print(row)
                if row[2] == source_mat_id:
                    return (row[0].split("_")[-1], row[1])
            elif math.isnan(row[0]):
                # Not all samples with an EMO BON code were sent to sequencing
                continue
    else:
        print(f"Cannot find run_id {source_mat_id} in any of the run information files")
        return (None, None)
