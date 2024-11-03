#! /usr/bin/env python3

from pathlib import Path
import sys
import os
import logging as log
import argparse
import textwrap
import subprocess
import shutil
import psutil

desc = """
Create Ghost Archives for MetaGOflow Data Products

This script creates a 'ghost' archive of a bzipped
tarball of the MetaGOflow run archive where the archive
consists of all the files but without any file content
"""


def main(
    target_directory, remove_open_archives=False, do_not_fix_archive=False, debug=False
):
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
        # Where the ghost archives will go
        Path("ghost-archives").mkdir(exist_ok=True)
        Path("fixed-archives").mkdir(exist_ok=True)

    # Loop through the tarball files
    for tarball_file in tarball_files:
        archive_without_top_level = False

        run_id = Path(str(tarball_file).rsplit(".", 2)[0])
        log.debug(f"run_id = {run_id}")
        ghost_dir = Path("ghost-archives", f"{run_id}")
        if ghost_dir.exists():
            log.info(f"Ghost archive already exists for {run_id}")
            continue

        if run_id.exists():
            log.debug("Found open archive")
        else:
            # All this flaffing about with a ./temp dir is because
            # there maybe other files in the to directory of the tarball
            # and we want to ensure those files go into the ghost
            # archive even if they never go to S3

            # For untaring archives
            Path("temp").mkdir(exist_ok=False)
            os.chdir("temp")

            # Find best bzip2 programme
            if shutil.which("lbzip2"):
                # Get nunmber of threads/cpu cores
                threads = psutil.cpu_count() - 2
                log.info(f"Using lbzip2 with {threads} threads")
                bzip2_program = f"lbzip2 -n {threads}"
            elif shutil.which("bzip2"):
                log.info("Using bzip2")
                bzip2_program = "bzip2"
            else:
                log.error("Cannot find lbzip2 or bzip2")
                log.error("Exiting...")
                sys.exit()

            log.info(f"Opening archive {tarball_file} with {bzip2_program}")
            # tar --use-compress-program lbunzip2 -xvf ../HMNJKDSX3.UDI200.tar.bz2
            subprocess.call(
                [
                    "tar",
                    "--use-compress-program",
                    {bzip2_program},
                    "-xf",
                    f"../{tarball_file}",
                ]
            )

            # Check archive
            if run_id.exists():
                os.chdir("..")
                # Move archive up to target directory
                Path("temp", run_id).rename(run_id)
                shutil.rmtree("temp")
            else:
                archive_without_top_level = True
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
                    tarball_file.rename(f"{tarball_file}-broken")
                    continue

        # Recursive glob the open archive
        archive_files = run_id.rglob("*")

        # Make ghost archive structure
        ghost_dir.mkdir(exist_ok=False)
        Path(ghost_dir, "results").mkdir()
        Path(ghost_dir, "results", "sequence-categorisation").mkdir()
        Path(ghost_dir, "results", "functional-annotation", "stats").mkdir(parents=True)
        Path(ghost_dir, "results", "taxonomy-summary", "LSU").mkdir(parents=True)
        Path(ghost_dir, "results", "taxonomy-summary", "SSU").mkdir()
        for af in archive_files:
            nfp = Path(ghost_dir, *af.parts[1:])
            log.debug(f"Writing {nfp}")
            nfp.touch()
        log.info(f"Created ghost archive for {run_id}")

        # Remove the open archive
        if not do_not_fix_archive and archive_without_top_level:
            # Double neg: fix it
            log.info(f"Fixing archive without top level directory {run_id}")
            subprocess.call(
                [
                    "tar",
                    "--use-compress-program",
                    bzip2_program,
                    "-cf",
                    f"fixed-archives/{run_id}.tar.bz2",
                    f"{run_id}",
                ]
            )
        if remove_open_archives:
            shutil.rmtree(run_id)

    # CD back to home directory
    log.info(f"Changing directory to {home_dir}")
    os.chdir(home_dir)

    log.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )
    parser.add_argument(
        "target_directory",
        help="Name of target directory containing MetaGOflow output",
    )
    parser.add_argument("-d", "--debug", action="store_true", help="DEBUG logging")
    parser.add_argument(
        "-r",
        "--remove_open_archives",
        action="store_true",
        help="Remove the open archives after creating the ghost archives",
    )
    parser.add_argument(
        "-nf",
        "--do_not_fix_archive",
        action="store_true",
        help="Do not fix archives that lack a top level directory",
    )
    args = parser.parse_args()
    main(
        args.target_directory,
        args.remove_open_archives,
        args.do_not_fix_archive,
        args.debug,
    )
