#! /usr/bin/env python3

import subprocess

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
        path: ./functional-annotation/{PREFIX}.merged.summary.go
        mime: text/csv
        header: ID,sub_process,process,abundance
      ips_annotations: 
        path: ./functional-annotation/{PREFIX}.merged.summary.ips
        mime: text/csv
        header: Abundance,ID,sequence_domain_region_name
      kegg_annotations: 
        path: ./functional-annotation/{PREFIX}.merged.summary.ko
        mime: text/csv
        header: Abundance,ID,name
      pfam_annotations: 
        path: ./functional-annotation/{PREFIX}.merged.summary.pfam
        mime: text/csv
        header: Abundance,ID,name
      eggnog_annotations: 
        path: ./functional-annotation/{PREFIX}.merged.emapper.summary.eggnog
        mime: text/csv
        header: Abundance,ID,name
    sink: ./{GENOSCOPE_ID}/functional-annotation
    template_name: functional-annotation.ldt.ttl
    mode: no-it
  - source: 
      path: ./taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.csv
      mime: text/csv
      header: OTU_ID,LSU_rRNA,taxonomy,taxid
    sink: ./{GENOSCOPE_ID}/taxonomy-summary
    template_name: taxon-info.ldt.ttl
"""


def run_apptainer(config):
    """
    This function runs the apptainer command with the specified arguments.
    It captures the output and error messages in a log file.
    """
    with open("/tmp/output.log", "a") as output:
        cmd = "apptainer run "
        subprocess.call(cmd, shell=True, stdout=output, stderr=output)


def write_work_yml_file(config):
    """
    This function writes the work YAML file with the specified configuration.
    """
    with open("./work.yml", "w") as work_file:
        work_file.write("work:\n")
        work_file.write(WORK_YML_TEMPLATE.format(**config))
        work_file.write("\n")


if __name__ == "__main__":
    # Example configuration dictionary
    config = {
        "PREFIX": "DBH",
        "CLUSTER_ID": "analysis-results-cluster01-crate",
        "GENOSCOPE_ID": "HWLTKDRXY.UDI211",  # target_directory
        "ENA_NR": "ENANUMBER123",  # conf["ena_accession_number"
        "SOURCE_MAT_ID": "EMOBON_NRMCB_So_7",  # conf["source_material_id"]
        "OBS_ID": "NRMCB",  # conf["obs_id"]
        "ENVPACKAGE_ID": "Se",  # conf["env_package_id"]
        "DOMAIN": "https://data.emobon.embrc.eu",
        "REPO_NAME": "analysis-results-cluster01-crate",
    }

    # Write the work YAML file
    write_work_yml_file(config)

    # Run the apptainer command
    # run_apptainer(config)
