#! /usr/bin/env python3

import argparse
import os
import sys
import json
import subprocess
from pathlib import Path
import logging


desc = """
This script delete the S3 objects for a single EMO BON ro-crate.

Assumes your AWS credentials are set in .aws/credentials and named [$profile_name]
"""


def extract_filenames_from_rocrate(rocrate_path, bucket, endpoint):
    """
    Extract the filenames from the ro-crate-metadata.json file.
    """

    filenames = []

    if not os.path.exists(rocrate_path):
        log.error(f"File {rocrate_path} does not exist.")
        sys.exit(1)
    with open(rocrate_path, "r") as f:
        data = f.read()
    try:
        data = json.loads(data)
    except json.JSONDecodeError:
        log.error(f"File {rocrate_path} is not a valid json file.")
        sys.exit(1)

    stanzas = data["@graph"]
    for stanza in stanzas:
        if "downloadUrl" in stanza:
            durl = stanza["downloadUrl"]
            if durl.startswith(f"{endpoint}"):
                filenames.append(durl)
                log.debug(f"Found file: {durl}")

    return filenames


def main(rocrate_path, bucket, profile_name, endpoint, remove):
    dry_run = "" if remove else "--dry-run"

    filenames = extract_filenames_from_rocrate(rocrate_path, bucket, endpoint)
    log.info(f"Found {len(filenames)} files to delete.")

    for fp in filenames:
        sp = Path(fp).relative_to(f"{endpoint}")
        # Command is dry-run and json output
        # {"operation":"rm","success":true,"source":"s3://mgf-data-products/files/md5/b0/993e236c97ed7308d1d93896dbc649"}
        cmd = f"s5cmd {dry_run} --json --profile {profile_name} --endpoint-url {endpoint} rm s3://{sp}"
        log.debug(f"Running command: {cmd}")
        result = subprocess.run(cmd, shell=True, capture_output=True)
        if result.returncode != 0:
            log.error(f"Error deleting {fp}: {result.stderr.decode()}")
            continue
        output = result.stdout.decode()
        try:
            output = json.loads(output)
        except json.JSONDecodeError:
            log.error(f"Error parsing json output: {output}")
            continue
        if output["success"]:
            log.info(f"Deleted {fp}")
        else:
            log.error(f"Error deleting {fp}: {output['error']}")
    if dry_run:
        log.info("THIS WAS A DRY RUN. NO FILES WERE DELETED")
        log.info("To delete the files, run the command with -r --remove flag set True")
    log.info("Done")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument(
        "rocrate_path",
        type=str,
        help="Path to the ro-crate-metadata.json file",
    )
    # Add arguement for dry-run
    parser.add_argument(
        "-r",
        "--remove",
        action="store_true",
        default=False,
        help="Remove files (default is False, a dry run)",
    )
    parser.add_argument(
        "-b",
        "--bucket",
        type=str,
        help="S3 bucket name",
        default="mgf-data-products",
    )
    parser.add_argument(
        "-p",
        "--profile_name",
        type=str,
        help="S3 profile name",
        default="eosc-fairease1",
    )
    parser.add_argument(
        "-e",
        "--endpoint",
        type=str,
        help="S3 endpoint",
        default="https://s3.mesocentre.uca.fr",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug mode",
    )

    args = parser.parse_args()

    # Logging
    log = logging.getLogger(__name__)
    log_level = logging.DEBUG if args.debug else logging.INFO
    logging.basicConfig(format="\t%(levelname)s: %(message)s", level=log_level)

    main(args.rocrate_path, args.bucket, args.profile_name, args.endpoint, args.remove)
