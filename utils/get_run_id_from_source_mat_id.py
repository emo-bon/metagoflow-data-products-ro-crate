#! /usr/bin/env python3

import sys

from utils import get_run_id_and_ref_code_from_source_mat_id as main

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python get_run_id_from_source_mat_id.py <source_mat_id>")
        sys.exit(1)

    source_mat_id = sys.argv[1]
    if not source_mat_id:
        print("source_mat_id cannot be empty.")
        sys.exit(1)

    # Call the main function with the provided run_id
    run_id, ref_code = main(source_mat_id)

    print(f"Run ID: {run_id}")
    print(f"Ref Code: {ref_code}")
