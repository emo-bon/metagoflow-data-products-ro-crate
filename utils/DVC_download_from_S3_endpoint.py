#!/usr/bin/env python3

import dvc.api
from pathlib import Path

def return_data_file_from_dvc(path_to_data_file):
    """
    Retrieve a data file from the S3 store.

    path_to_data_file should be a string relative to the DVC repository,
    e.g. EMOBON_HCMR-1_Wa_6-ro-crate/taxonomy-summary/SSU/SSU-taxonomy-summary.ttl
    (without the ".dvc" suffix)

    where the repository is
    "metagoflow-data-products-ro-crate/analysis-results-cluster-01-crate"

    """

    path_to_dvc_repo = Path("../analysis-results-cluster-01-crate")

    #Check that file exists
    file_path = path_to_dvc_repo / path_to_data_file
    dvc_path = file_path.with_suffix(file_path.suffix + ".dvc")
    if not dvc_path.exists():
        print(f"ERROR: {dvc_path} does not exist")


    # Read the file content
    return dvc.api.read(
        path_to_data_file,
        repo=path_to_dvc_repo,
        remote="myremote"
    )


