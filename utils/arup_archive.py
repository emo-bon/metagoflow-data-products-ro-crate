#! /usr/bin/env python3

import subprocess
from pathlib import Path

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
    value: "{DOMAIN}/{REPO_NAME}/{SOURCE_MAT_ID}-ro-crate"
  - name: prefix
    value: "{PREFIX}"

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
    sink: ./{GENOSCOPE_ID}/results/functional-annotation
    template_name: functional-annotation.ldt.ttl
    mode: no-it
  - source: 
      path: ./results/taxonomy-summary/LSU/{PREFIX}.merged_LSU.fasta.mseq.tsv
      mime: text/csv
      header: OTU_ID,LSU_rRNA,taxonomy,taxid
    sink: ./{GENOSCOPE_ID}/results/taxonomy-summary
    template_name: taxon-info.ldt.ttl
"""


def run_apptainer(data_path=TEST_DATA_PATH):
    """
    This function runs the apptainer command with the specified arguments.
    It captures the output and error messages in a log file.
    """
    with open("./apptainer_output.log", "a") as output:
        cmd = (
            "export ARUP_WORK=./work.yml && apptainer run "
            f"--bind {data_path}:/rocrateroot "
            "emobon_arup.sif"
        )

        subprocess.call(cmd, shell=True, stdout=output, stderr=output)


def write_work_yml_file(config, filepath):
    """
    This function writes the work YAML file with the specified configuration.
    """
    with open(Path(filepath, "./work.yml"), "w") as work_file:
        work_file.write(WORK_YML_TEMPLATE.format(**config))
        work_file.write("\n")


if __name__ == "__main__":
    # Example configuration dictionary
    config = {
        "PREFIX": "DBB",
        "CLUSTER_ID": "analysis-results-cluster01-crate",
        "GENOSCOPE_ID": "HVWGWDSX5.UDI134",  # target_directory
        "ENA_NR": "ENANUMBER123",  # conf["ena_accession_number"
        "SOURCE_MAT_ID": "EMOBON_EMT21_Wa_22",  # conf["source_material_id"]
        "OBS_ID": "EMT21",  # conf["obs_id"]
        "ENVPACKAGE_ID": "Wa",  # conf["env_package_id"]
        "DOMAIN": "https://data.emobon.embrc.eu",
        "REPO_NAME": "analysis-results-cluster01-crate",
    }

    # Write the work YAML file
    write_work_yml_file(config, filepath=TEST_DATA_PATH)

    # Run the apptainer command
    run_apptainer()
