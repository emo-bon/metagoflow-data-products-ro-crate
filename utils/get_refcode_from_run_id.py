#! /usr/bin/env python3

import sys

from utils import get_refcode_and_source_mat_id_from_run_id as main

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python refcode_from_run_id.py <run_id>")
        sys.exit(1)

    run_id = sys.argv[1]
    if not run_id:
        print("Run ID cannot be empty.")
        sys.exit(1)

    # Call the main function with the provided run_id
    ref_code, source_mat_id = main(run_id)

    print(f"Ref Code: {ref_code}")
    print(f"Source Material ID: {source_mat_id}")
