#! /usr/bin/env python3

import subprocess
import argparse
import textwrap
from pathlib import Path
import logging as log

desc = """
Analysis Results UPlifing - ARUP

Uses the container from this repository:
https://github.com/emo-bon/analysis-results-uplifting-docker

Container is converted to a Apptainer image using the
./utils/install_apptainer.sh script

If run as a script, it will create a work.yml file and run a test with data in
the ../tests/arup/data/HVWGWDSX5.UDI134 directory.

"""

# Relative to the utils directory
TEST_DATA_PATH = "../tests/arup/data/HVWGWDSX5.UDI134"

WORK_YML_TEMPLATE = """
vars:
  - name: cluster
    value: "{CLUSTER_ID}"
  - name: genoscopeID
    value: "{GENOSCOPE_ID}"
  - name: enanumber
    value: "{ENA_NR}"
  - name: obs_id
    value: "{OBS_ID}"
  - name: env_package
    value: "{ENVPACKAGE_ID}"
  - name: source_mat_id
    value: "{SOURCE_MAT_ID}"
  - name: uri_to_crate
    value: "{DOMAIN}/{CLUSTER_ID}/{SOURCE_MAT_ID}-ro-crate"

subyt:
  - extra_sources:
      go_annotations:
        path: ./results/functional-annotation/{PREFIX}.merged.summary.go
        mime: text/csv
        header: ID,sub_process,process,abundance
      ips_annotations: 
        path: ./results/functional-annotation/{PREFIX}.merged.summary.ips
        mime: text/csv
        header: Abundance,ID,sequence_domain_region_name
      kegg_annotations: 
        path: ./results/functional-annotation/{PREFIX}.merged.summary.ko
        mime: text/csv
        header: Abundance,ID,name
      pfam_annotations: 
        path: ./results/functional-annotation/{PREFIX}.merged.summary.pfam
        mime: text/csv
        header: Abundance,ID,name
      eggnog_annotations: 
        path: ./results/functional-annotation/{PREFIX}.merged.emapper.summary.eggnog
        mime: text/csv
        header: Abundance,ID,name
    sink: ./results/functional-annotation/functional-annotation.ttl
    template_name: functional-annotation.ldt.ttl
    mode: no-it
  - source: 
      path: ./results/taxonomy-summary/LSU/{PREFIX}.merged_LSU.fasta.mseq.tsv
      mime: text/csv
      delimiter: "\t"
      header: "OTU_ID\tLSU_rRNA\ttaxonomy\ttaxid"
    sink: ./results/taxonomy-summary/LSU-taxonomy-summary.ttl
    template_name: taxon-info.ldt.ttl
  - source: 
      path: ./results/taxonomy-summary/SSU/{PREFIX}.merged_SSU.fasta.mseq.tsv
      mime: text/csv
      delimiter: "\t"
      header: "OTU_ID\tSSU_rRNA\ttaxonomy\ttaxid"
    sink: ./results/taxonomy-summary/SSU-taxonomy-summary.ttl
    template_name: taxon-info.ldt.ttl
"""


def run_apptainer(path_to_data):
    """
    This function runs the apptainer command with the specified arguments.
    It captures the output and error messages in a log file.
    """
    work_yml_path = Path(path_to_data, "work.yml")
    log.debug(f"work_yml_path: {work_yml_path}")
    with open("./apptainer_output.log", "a") as output:
        cmd = (
            f'export ARUP_WORK="./work.yml" && apptainer run '
            f"--bind {path_to_data}:/rocrateroot "
            "emobon_arup.sif"
        )
        subprocess.call(cmd, shell=True, stdout=output, stderr=output)
        log.info("Apptainer command executed successfully")


def write_work_yml_file(config, path_to_data):
    """
    This function writes the work YAML file with the specified configuration.
    """
    with open(Path(path_to_data, "work.yml"), "w") as work_file:
        work_file.write(WORK_YML_TEMPLATE.format(**config))
        work_file.write("\n")


def main(config, path_to_data, debug=False):
    """
    Main function to run the script
    It creates a work YAML file and runs the apptainer command

    config is a dictionary with the following keys:
    - PREFIX: Prefix for the files
    - CLUSTER_ID: Cluster ID
    - GENOSCOPE_ID: Genoscope ID
    - ENA_NR: ENA number
    - SOURCE_MAT_ID: Source material ID
    - OBS_ID: Observation ID
    - ENVPACKAGE_ID: Environment package ID
    - DOMAIN: Domain for the URI

    """

    log.basicConfig(
        format="\t%(levelname)s: %(message)s", level=log.DEBUG if debug else log.INFO
    )

    # Check if the path to data exists
    path_to_data = Path(path_to_data)
    if not path_to_data.exists():
        raise FileNotFoundError(f"Path to data does not exist: {path_to_data}")
    log.debug(f"Found path to data: {path_to_data}")

    # Check if ttl files aleady exist
    fa_ttl = Path(
        path_to_data, "results", "functional-annotation", "functional-annotation.ttl"
    )
    tax_ttl = Path(path_to_data, "results", "taxonomy-summary", "taxonomy-summary.ttl")
    if fa_ttl.exists():
        log.info("Functional Analysis TTL file already exists: removing...")
        fa_ttl.unlink()
    if tax_ttl.exists():
        log.info("Taxonomy Summary TTL file already exists: removing...")
        tax_ttl.unlink()

    # Check config keys
    required_keys = [
        "PREFIX",
        "CLUSTER_ID",
        "GENOSCOPE_ID",
        "ENA_NR",
        "SOURCE_MAT_ID",
        "OBS_ID",
        "ENVPACKAGE_ID",
        "DOMAIN",
    ]
    for key in required_keys:
        if key not in config:
            raise KeyError(f"Missing required config key: {key}")
    # Check if the config values are not empty
    for key, value in config.items():
        if not value:
            raise ValueError(f"Config key {key} has an empty value")
        if not isinstance(value, str):
            raise TypeError(f"Config key {key} is not a string")

    log.debug(f"Config: {config}")

    # Write the work YAML file
    write_work_yml_file(config, path_to_data)
    # Move yml con
    # Run the apptainer command
    run_apptainer(path_to_data)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )
    parser.add_argument("-d", "--debug", action="store_true", help="DEBUG logging")

    # Example configuration dictionary
    config = {
        # PREFIX is the GENOSCOPE Project Code that prefixes the sequence result files
        # "DBH_AAAVOSDA_1_1_HCFCYDSX5.UDI141_clean.fastq.gz is DBH"
        # This varies
        "PREFIX": "DBB",
        # CLUSTER_ID is name of the Github respository where the RO-Crates for the
        # 'cluster' (effectively just several batches) are going to be stored
        # e.g. https://github.com/emo-bon/analysis-results-cluster-01-crate
        # will store Batch 1 and Batch 2 RO-Crates
        "CLUSTER_ID": "analysis-results-cluster01-crate",
        # This is in fact the Flowcell id (e.g. HCFCYDSX5) and Index id (e.g. UDI141)
        # of the sequencing machine where the sample was processed
        # e.g. HCFCYDSX5.UDI141
        # It is unique and used to name the MGF result archives
        # e.g. HJWK3DSX7.UDI074.tar.bz2
        "GENOSCOPE_ID": "HVWGWDSX5.UDI134",  # target_directory
        # This is the EMBL ENA Accesion number
        "ENA_NR": "ENANUMBER123",  # conf["ena_accession_number"]
        # This is the source material id
        # This is the unique identifier created by the Observarory sample sheets
        "SOURCE_MAT_ID": "EMOBON_EMT21_Wa_22",  # conf["source_material_id"]
        # This is the abbreviated Observatory id
        "OBS_ID": "EMT21",  # conf["obs_id"]
        # This is the shortened version of the environment package id
        # id's are "water_column" or "soft_sediment", here abrreviated to
        # "Wa" and "Ss"
        "ENVPACKAGE_ID": "Wa",  # conf["env_package_id"]
        # This is the domain URI of the EMO BON data repository
        "DOMAIN": "https://data.emobon.embrc.eu",
    }

    # Run test
    args = parser.parse_args()
    main(config, TEST_DATA_PATH, args.debug)
