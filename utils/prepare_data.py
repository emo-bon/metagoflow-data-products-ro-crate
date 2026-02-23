#! /usr/bin/env python3

from pathlib import Path
import sys
import os
import logging as log
import argparse
import textwrap
import subprocess
import psutil

from utils import find_bzip2, open_archive, get_refcode_and_source_mat_id_from_run_id

desc = """
Prepare the MGF data archives for the ro-crate building. All files in the target
directory will be opened and the sequence files compressed, and moved to the
current working directory.

This script opens the MGF results archive and compresses the individual data
files from building the ro-crate.
"""

FILE_PATTERNS = [
    "*.fastq.trimmed.fasta",
    "*.merged_CDS.faa",
    "*.merged_CDS.ffn",
    "*.merged.cmsearch.all.tblout.deoverlapped",
    "*.merged.fasta",
    "*.merged.motus.tsv",
    "*.merged.unfiltered_fasta",
    "final.contigs.fa",
]

# RO_CRATE_REPO_PATH = "../analysis-results-cluster-01-crate"
RO_CRATE_REPO_PATH = "../analysis-results-cluster-02-crate"

def get_existing_rorates():
    """Return a list of existing ro-crates"""
    utils_path = Path(__file__).resolve()
    utils_dir = utils_path.parent
    path_to_rocrates = Path(utils_dir, RO_CRATE_REPO_PATH)
    log.debug(f"path_to_rocrates: {path_to_rocrates}")
    existing_rocrates_paths = list(Path(path_to_rocrates).glob("*-ro-crate"))
    existing_rocrates_names = [p.name for p in existing_rocrates_paths]
    log.debug(f"Existing rocrate names: {existing_rocrates_names}")
    return existing_rocrates_names


def main(
    target_directory,
    max_num,
    debug=False,
):
    log.basicConfig(
        format="\t%(levelname)s: %(message)s", level=log.DEBUG if debug else log.INFO
    )

    home_dir = Path.cwd()

    # Check the target_directory name
    target_directory = Path(target_directory)
    if not target_directory.exists():
        log.error(f"Cannot find the target directory {target_directory}")
        sys.exit()
    log.debug(f"Found target directory {target_directory}")

    # Check that it is a directory:
    if not target_directory.is_dir():
        log.error(f"Target directory {target_directory} is not a directory")
        sys.exit()

    # Get list of tarball files
    tarball_files = list(Path(target_directory).glob("*.tar.bz2"))

    log.debug(f"Found {len(tarball_files)} tarball files")
    for tarball in tarball_files:
        log.debug(f"Tarball name: {tarball.name}")
    if len(tarball_files) == 0:
        log.error(f"Cannot find any tarball files in {target_directory}")
        sys.exit()

    # Where the open archives will go
    outpath = Path("prepared_archives")
    if outpath.exists():
        log.debug("'prepared_archives' directory already exists")
    else:
        log.debug("Creating 'prepared_archives' directory")
        outpath.mkdir()
    # Change to output directory
    log.debug(f"Changing directory to {outpath}")

    os.chdir(outpath)
    working_dir = Path.cwd()

    existing_rocrates_names = get_existing_rorates()

    bzip2_program = find_bzip2()

    # Loop through the tarball files
    count = 0
    for tarball_file in tarball_files:
        log.debug(f"Preparing tarball_file: {tarball_file}")

        run_id = Path(str(tarball_file.name).rsplit(".", 2)[0].strip())

        # First check to see if a ro-crate already exists
        log.debug(f"Looking for ref_code and source_mat_id of {run_id}")
        ref_code, source_mat_id = get_refcode_and_source_mat_id_from_run_id(
            str(run_id)
        )  # Need str(run_id) because its a Path()
        if not ref_code:
            log.error(f"ref_code for {run_id} not found")
            sys.exit()
        if not source_mat_id:
            log.error(f"source_mat_id for {run_id} not found")
            sys.exit()
        log.info(f"Source_mat_id for {run_id} = {source_mat_id}")
        rocrate_name = source_mat_id + "-ro-crate"
        if rocrate_name in existing_rocrates_names:
            log.info(f"RO-Crate already exists: {rocrate_name}... continuing")
            continue
        elif run_id.exists():
            log.info(f"Found existing prepared archive {run_id}... continuing")
            continue
        else:
            if count == max_num:
                log.info(f"{max_num} samples have been opened - stopping")
                break
            else:
                count += 1

        # Open the archive
        log.info(f"Opening archive {tarball_file}")
        open_archive(tarball_file, bzip2_program)
        path_to_results = Path(str(run_id), "results")
        if not path_to_results.exists():
            log.error(f"Unable to open {tarball_file}")
            continue

        log.debug(f"Path to results: {path_to_results}")
        os.chdir(path_to_results)
        log.debug(f"CWD: {os.getcwd()}")
        # Compress the sequence archive files
        log.info(f"Compressing sequence files for {run_id}")

        # Deal with MOTUS - sometimes it's empty and has the name empty.motus.tsv
        if Path("./empty.motus.tsv").exists():
            # Get prefix
            sf = list(Path("./").glob("*.merged.fasta"))
            prefix = sf[0].parts[0].split(".")[0]
            # Change file name
            src = Path("./empty.motus.tsv")
            dest = src.with_name(f"{prefix}.merged.motus.tsv")
            src.rename(dest)

        for fp in FILE_PATTERNS:
            sf = Path("./").glob(fp)
            for f in sf:
                log.debug(f"Compressing {f}")
                # Can't use f{} style formatting in subprocess call
                # of the program name
                if bzip2_program == "lbzip2":
                    threads = psutil.cpu_count() - 4
                    log.debug(f"Using lbzip2 with {threads} threads")
                    subprocess.check_call(
                        [
                            "lbzip2",
                            "-9",
                            f"-n {threads}",
                            f"./{f}",
                        ]
                    )
                elif bzip2_program == "bzip2":
                    log.debug(f"bzip2 -9 {f}")
                    subprocess.check_call(
                        [
                            "bzip2",
                            "-9",
                            f"./{f}",
                        ]
                    )
        os.chdir(working_dir)
        log.info(f"Finished writing {rocrate_name}")

    # CD back to home directory
    log.debug(f"Changing directory to {home_dir}")
    os.chdir(home_dir)
    log.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )
    parser.add_argument(
        "target_directory",
        help=(
            "Name of target directory containing MetaGOflow output"
            " archives to prepare relative to the current working directory"
        ),
    )
    parser.add_argument(
        "-n",
        "--max_num",
        help="Maximum number of runs to process",
        default=10,
        type=int,
    )
    parser.add_argument("-d", "--debug", action="store_true", help="DEBUG logging")
    args = parser.parse_args()
    main(
        args.target_directory,
        args.max_num,
        args.debug,
    )
