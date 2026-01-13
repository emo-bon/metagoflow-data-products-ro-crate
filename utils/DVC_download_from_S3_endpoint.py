#!/usr/bin/env python3

import dvc.api
import os

# Path to the file in the DVC repository (relative to the repo root)
dvc_path = "EMOBON_HCMR-1_Wa_6-ro-crate/taxonomy-summary/SSU/SSU-taxonomy-summary.ttl"

# Read the file content
content = dvc.api.read(
    dvc_path,
    repo="/home/cymon/vscode/git-repos/metagoflow-data-products-ro-crate/analysis-results-cluster-01-crate",
    remote="myremote"  # Use the remote name you configured
)

print(f"{content}")

## Try to write as text (for YAML, CSV, TXT, etc.)
#try:
#    with open(local_output_path, "w", encoding="utf-8") as f:
#        f.write(file_content)
## If it fails, write as binary (for images, PDFs, etc.)
#except TypeError:
#    with open(local_output_path, "wb") as f:
#        f.write(file_content)


