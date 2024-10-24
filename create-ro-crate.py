#! /usr/bin/env python3

import os
import math
import argparse
import textwrap
import sys
import yaml
import json
import datetime
import requests
import tempfile
import shutil
import glob
import subprocess
import logging as log
from pathlib import Path

import pandas as pd

desc = """
Build a MetaGOflow Data Products ro-crate from a YAML configuration.

Invoke
$ create-ro-crate.py <target_directory> <yaml_configuration>

where:
    target_directory is the toplevel output directory of MetaGOflow
        This must be the MGF run_id, e.g. HWLTKDRXY-UDI210
        Note that the name of the directory cannot have a period "." in it!
    yaml_configuration is a YAML file of metadata specific to this ro-crate
        a template is here:
        https://raw.githubusercontent.com/emo-bon/MetaGOflow-Data-Products-RO-Crate/main/ro-crate-config.yaml

e.g.

$ create-ro-crate.py HWLTKDRXY-UDI210 config.yml

This script expects to be pointed to directory of MetaGOflow output.

When invoked, the MetaGOflow run_wf.sh script writes all output to a directory specified by
the "-d" parameter:

    $ run_wf.sh -n green -d  HWLTKDRXY-UDI210 -f input_data/${DATA_FORWARD} -r input_data/${DATA_REVERSE}

    $ tree -1
    HWLTKDRXY-UDI210
    ├── prov
    ├── results
    ├── green.yml
    └── tmp

    3 directories, 1 file

Cymon J. Cox, Feb '23
"""

#########################################################################################
# These are the paths to the external data files that are used to build the RO-Crate
# run-information files for each batch - these return the ref_code for a given MGF run_id
BATCH1_RUN_INFO_PATH = (
    "https://raw.githubusercontent.com/emo-bon/sequencing-data/main/shipment/"
    "batch-001/run-information-batch-001.csv"
)
BATCH2_RUN_INFO_PATH = (
    "https://raw.githubusercontent.com/emo-bon/sequencing-data/main/shipment/"
    "batch-002/run-information-batch-002.csv"
)
# ENA ACCESSSION INFO for each batch
BATCH1_ENA_ACCESSION_INFO_PATH = (
    "https://raw.githubusercontent.com/emo-bon/sequencing-data/refs/heads/main/"
    "shipment/batch-001/ena-accession-numbers-batch-001.csv"
)
BATCH2_ENA_ACCESSION_INFO_PATH = (
    "https://raw.githubusercontent.com/emo-bon/sequencing-data/refs/heads/main/"
    "shipment/batch-002/ena-accession-numbers-batch-002.csv"
)
# The combined sampling event logsheets for batch 1 and 2
COMBINED_LOGSHEETS_PATH = (
    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
    "refs/heads/main/validated-data/Batch1and2_combined_logsheets_2024-09-19.csv"
)
OBSERVATORY_LOGSHEETS_PATH = (
    "https://raw.githubusercontent.com/emo-bon/emo-bon-data-validation/"
    "refs/heads/main/validated-data/Observatory_combined_logsheets_validated.csv"
)
# The ro-crate metadata template from Github
TEMPLATE_URL = (
    "https://raw.githubusercontent.com/emo-bon/MetaGOflow-Data-Products-RO-Crate"
    "/main/ro-crate-metadata.json-template"
)
# The MGF _Run_Track Google Sheets
FILTERS_MGF_PATH = (
    "https://docs.google.com/spreadsheets/d/"
    "1j9tRRsRCcyViDMTB1X7lx8POY1P5bV7UijxKKSebZAM/gviz/tq?tqx=out:csv&sheet=FILTERS"
)
SEDIMENTS_MGF_PATH = (
    "https://docs.google.com/spreadsheets/d/"
    "1j9tRRsRCcyViDMTB1X7lx8POY1P5bV7UijxKKSebZAM/gviz/tq?tqx=out:csv&sheet=SEDIMENTS"
)

# S3 store path
S3_STORE_URL = "http://s3.mesocentre.uca.fr/mgf-data-products"

# This is the workflow YAML file, the prefix is the "-n" parameter of the
# "run_wf.sh" script:
WORKFLOW_YAML_FILENAME = "{run_parameter}.yml"
CONFIG_YAML_PARAMETERS = [
    "datePublished",
    "run_parameter",
    "metagoflow_version",
    "missing_files",
]

MANDATORY_FILES = [
    "fastp.html",
    "final.contigs.fa",
    "RNA-counts",
    "functional-annotation/stats/go.stats",
    "functional-annotation/stats/interproscan.stats",
    "functional-annotation/stats/ko.stats",
    "functional-annotation/stats/orf.stats",
    "functional-annotation/stats/pfam.stats",
    "taxonomy-summary/LSU/krona.html",
    "taxonomy-summary/SSU/krona.html",
    "functional-annotation/{prefix}.merged_CDS.I5.tsv.gz",
    "functional-annotation/{prefix}.merged.hmm.tsv.gz",
    "functional-annotation/{prefix}.merged.summary.go",
    "functional-annotation/{prefix}.merged.summary.go_slim",
    "functional-annotation/{prefix}.merged.summary.ips",
    "functional-annotation/{prefix}.merged.summary.ko",
    "functional-annotation/{prefix}.merged.summary.pfam",
    "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq.gz",
    "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq_hdf5.biom",
    "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq_json.biom",
    "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq.tsv",
    "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq.txt",
    "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq.gz",
    "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq_hdf5.biom",
    "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq_json.biom",
    "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq.tsv",
    "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq.txt",
]

YAML_ERROR = """
Cannot find the run YAML file. Bailing...

If you invoked run_wf.sh like this, then the YAML configuration file will be
named "green.yml in the "HWLTKDRXY.UDI210" directory:

    $ run_wf.sh -n green -d  HWLTKDRXY.UDI210 \
                -f input_data/${DATA_FORWARD} \
                -r input_data/${DATA_REVERSE}

Configure the "run_parameter" with "-n" parameter value in the config.yml file:
"run_parameter": "green"
"""


def get_ref_code_and_prefix(conf):
    """Get the reference code for a given run_id.
    run_id is the last part of the reads_name in the run information file.
    e.g. 'DBH_AAAAOSDA_1_HWLTKDRXY.UDI235' and is the name used to label the target_directory.
    """
    for i, batch in enumerate([BATCH1_RUN_INFO_PATH, BATCH2_RUN_INFO_PATH]):
        df = pd.read_csv(batch)
        for row in df[["reads_name", "ref_code"]].values.tolist():
            if isinstance(row[0], str):
                # print(row)
                if row[0].split("_")[-1] == conf["run_id"]:
                    conf["ref_code"] = row[1]
                    conf["prefix"] = row[0].split("_")[0]
                    conf["batch_number"] = i + 1
                    return conf
            elif math.isnan(row[0]):
                # Not all samples with an EMO BON code were sent to sequencing
                continue
    log.error("Cannot find the ref_code for run_id %s" % conf["run_id"])
    sys.exit()


def writeHTMLpreview():
    """Write the HTML preview file using rochtml-
    https://www.npmjs.com/package/ro-crate-html
    """
    rochtml_path = shutil.which("rochtml")
    if not rochtml_path:
        log.info(
            "HTML preview file cannot be written due to missing executable (rochtml)"
        )
    else:
        cmd = "%s %s" % (
            rochtml_path,
            Path("ro-crate-metadata.json"),
        )
        child = subprocess.Popen(
            str(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True
        )
        stdoutdata, stderrdata = child.communicate()
        return_code = child.returncode
        if return_code != 0:
            log.error("Error whilst trying write HTML file")
            log.error("Stderr: %s " % stderrdata)
            log.error("Command: %s" % cmd)
            log.error("Return code: %s" % return_code)
            log.error("Bailing...")
            sys.exit()
        else:
            log.info("Written HTML preview file")


def sequence_categorisation_stanzas(target_directory, template):
    """Glob the sequence_categorisation directory and build a stanza for each
    zipped data file

    Return updated template, and list of sequence category filenames
    """
    search = os.path.join(
        target_directory, "results", "sequence-categorisation", "*.gz"
    )
    seq_cat_paths = glob.glob(search)
    seq_cat_files = [os.path.split(f)[1] for f in seq_cat_paths]
    # Sequence-categorisation stanza
    for i, stanza in enumerate(template["@graph"]):
        if stanza["@id"] == "sequence-categorisation/":
            stanza["hasPart"] = [dict([("@id", fn)]) for fn in seq_cat_files]
            sq_index = i
            break

    seq_cat_files.reverse()
    for fn in seq_cat_files:
        link = os.path.join(
            S3_STORE_URL,
            target_directory + "%2F" + "sequence-categorisation" + "%2F" + fn,
        )
        d = dict(
            [
                ("@id", fn),
                ("@type", "File"),
                ("downloadUrl", link),
                ("encodingFormat", "application/zip"),
            ]
        )
        template["@graph"].insert(sq_index + 1, d)
    return template, seq_cat_files


def read_yaml(yaml_config):
    # Read the YAML configuration
    if not os.path.exists(yaml_config):
        log.error(f"YAML configuration file does not exist at {yaml_config}")
        sys.exit()
    with open(yaml_config, "r") as f:
        conf = yaml.safe_load(f)
    # Check yaml parameters are formated correctly, but not necessarily sane
    for param in CONFIG_YAML_PARAMETERS:
        log.debug("Config paramater: %s" % conf[param])
        if param == "datePublished":
            if conf[param] == "None":
                # No specified date, delete from conf
                # Its absence will trigger formatting
                # with today's date
                del conf[param]
                continue
            else:
                if not isinstance(conf[param], str):
                    log.error(
                        "'dataPublished' should either be a string or 'None'. Bailing..."
                    )
                    sys.exit()
                try:
                    datetime.datetime.fromisoformat(conf[param])
                except ValueError:
                    log.error(f"'datePublished' must conform to ISO 8601: {param}")
                    log.error("Bailing...")
                    sys.exit()
        elif param == "missing_files":
            if param not in conf:
                continue
            else:
                for filename in conf[param]:
                    if not isinstance(filename, str):
                        log.error(
                            f"Parameter '{filename}' in 'missing_files' list in YAML file must be a string."
                        )
                        log.error("Bailing...")
                        sys.exit()
        else:
            if not conf[param] or not isinstance(conf[param], str):
                log.error(f"Parameter '{param}' in YAML file must be a string.")
                log.error("Bailing...")
                sys.exit()
    log.info("YAML configuration looks good...")
    return conf


def check_and_format_data_file_paths(target_directory, conf, check_exists=True):
    """Check that all mandatory files are present in the target directory"""

    workflow_yaml_path = WORKFLOW_YAML_FILENAME.format(**conf)
    filepaths = [f.format(**conf) for f in MANDATORY_FILES]

    if check_exists:
        path = Path(target_directory, workflow_yaml_path)
        log.debug("Looking for worflow YAML file at: %s" % path)
        if not os.path.exists(path):
            log.error("Cannot find workflow YAML file at %s" % path)
            sys.exit()
        # The fixed file paths
        for filepath in filepaths:
            log.debug(f"File path: {filepath}")
            path = Path(target_directory, "results", filepath)
            if not os.path.exists(path):
                if "missing_files" in conf:
                    if os.path.split(filepath)[1] in conf["missing_files"]:
                        # This file is known to be missing, ignoring
                        log.info(
                            "Ignoring specified missing file: %s"
                            % os.path.split(filepath)[1]
                        )
                        filepaths.remove(filepath)
                        continue
                log.error(
                    "Could not find the mandatory file '%s' at the following path: %s"
                    % (filepath, path)
                )
                log.error(
                    "Consider adding it to the 'missing_files' list in the YAML configuration."
                )
                log.error("Cannot continue...")
                sys.exit()
            else:
                log.debug("Found %s" % path)

    log.info("Data look good...")
    filepaths.append(workflow_yaml_path)
    return filepaths


def get_persons_and_institution_data(conf):
    """Get the sampling person and institution for a given ref_code."""
    # Read the relevant row in sample sheet
    df_samp = pd.read_csv(COMBINED_LOGSHEETS_PATH)
    row_samp = df_samp.loc[df_samp["ref_code"] == conf["ref_code"]].to_dict()
    # Get the env_package either water_column or soft_sediments
    env_package = list(row_samp["env_package"].values())[0]
    # Get the observatory ID
    obs_id = list(row_samp["obs_id"].values())[0]

    # Get the observatory data
    df_obs = pd.read_csv(OBSERVATORY_LOGSHEETS_PATH)
    # Get the observatory data using the obs_id and env_package variables
    row_obs = df_obs.loc[
        (df_obs["obs_id"] == obs_id) & (df_obs["env_package"] == env_package)
    ].to_dict()

    # Sampling person name and ORCID
    conf["sampling_person_name"] = list(row_samp["sampl_person"].values())[0]
    conf["sampling_person_identifier"] = list(row_samp["sampl_person_orcid"].values())[
        0
    ]
    # Sampling person affiliation
    conf["sampling_person_station_edmoid"] = list(
        row_obs["organization_edmoid"].values()
    )[0]
    conf["sampling_person_station_name"] = list(row_obs["organization"].values())[0]
    conf["samling_person_station_country"] = list(row_obs["geo_loc_name"].values())[0]

    # Add MGF analysis creator_person
    mgf_path = FILTERS_MGF_PATH if env_package == "water_column" else SEDIMENTS_MGF_PATH
    data = pd.read_csv(mgf_path).to_dict(orient="records")
    for row in data:
        if row["ref_code"] == conf["ref_code"]:
            log.debug("Row in %s: %s" % (mgf_path, row))
            if row["who"] == "CCMAR":
                conf["creator_person_name"] = "Cymon J. Cox"
                conf["creator_person_identifier"] = "0000-0002-4927-979X"
                conf["creator_person_station_edmoid"] = "2516"
                conf["creator_person_station_name"] = (
                    "Centre of Marine Sciences (CCMAR)"
                )
                conf["creator_person_station_country"] = "Portugal"
            elif row["who"] == "HCMR":
                conf["creator_person"] = "Stelios Ninidakis"
                conf["creator_person_identifier"] = "0000-0003-3898-9451"
                conf["creator_person_station_edmoid"] = "141"
                conf["creator_person_station_name"] = (
                    "Institute of Marine Biology "
                    "Biotechnology and Aquaculture (IMBBC) Hellenic Centre "
                    "for Marine Research (HCMR)"
                )
                conf["creator_person_station_country"] = "Greece"
            else:
                log.error("Unrecognised creater of MGF data: %s" % row["who"])
                sys.exit()
            log.debug("MetaGOflow version: %s" % row["version"])
            conf["metagoflow_version_id"] = row["version"]
            break
    else:
        log.error(
            "Cannot find the creator of MGF data for ref_code %s" % conf["ref_code"]
        )
        sys.exit()

    return conf


def get_ena_accession_data(conf):
    """Get the ENA accession data for a given ref_code."""
    # Read the relevant row in sample sheet
    if conf["batch_number"] == 1:
        df_ena = pd.read_csv(BATCH1_ENA_ACCESSION_INFO_PATH)
    elif conf["batch_number"] == 2:
        df_ena = pd.read_csv(BATCH2_ENA_ACCESSION_INFO_PATH)
    else:
        log.error(f"Batch number not recognised {conf['batch_number']}")
        sys.exit()
    row_ena = df_ena.loc[df_ena["ref_code"] == conf["ref_code"]].to_dict()
    # Get the ENA accession data
    conf["ena_accession_number"] = list(
        row_ena["ena_accession_number_sample"].values()
    )[0]
    conf["ena_accession_number_url"] = (
        f"https://www.ebi.ac.uk/ena/browser/view/{conf['ena_accession_number']}"
    )
    return conf


def write_metadata_json(target_directory, conf, filepaths):
    metadata_json_template = "../ro-crate-metadata.json-template"
    if os.path.exists(metadata_json_template):
        log.debug("Using local metadata.json template")
        with open(metadata_json_template, "r") as f:
            template = json.load(f)
    else:
        # Grab the template from Github
        log.debug("Downloading metadata.json template from Github")
        req = requests.get(TEMPLATE_URL)
        if req.status_code == requests.codes.ok:
            template = req.json()
        else:
            log.error("Unable to download the metadata.json file from Github")
            log.error(f"Check {TEMPLATE_URL}")
            log.error("Bailing...")
            sys.exit()

    # print(json.dumps(template, indent=2))

    # Build the persons and institution stanzas
    conf = get_persons_and_institution_data(conf)
    conf = get_ena_accession_data(conf)
    log.debug("Conf dict: %s" % conf)

    log.info("Writing ro-crate-metadata.json...")

    # Add strings first
    # Add "ref_code"'s to "name", "title", and "description" fields
    template["@graph"][1]["name"] = template["@graph"][1]["name"].format(**conf)
    template["@graph"][1]["description"] = template["@graph"][1]["description"].format(
        **conf
    )
    template["@graph"][1]["title"] = template["@graph"][1]["title"].format(**conf)

    # Add date to "datePublished"
    if "datePublished" in conf:
        template["@graph"][1]["datePublished"] = template["@graph"][1][
            "datePublished"
        ].format(**conf)
    else:
        template["@graph"][1]["datePublished"] = datetime.datetime.now().strftime(
            "%Y-%m-%d"
        )

    # Add metadGOflow version id
    for section in template["@graph"]:
        if section["@id"] == "{metagoflow_version}":
            section["@id"] = section["@id"].format(**conf)
            section["softwareVersion"] = section["softwareVersion"].format(**conf)
            section["downloadUrl"] = section["downloadUrl"].format(**conf)
            break
    else:
        log.error("Cannot find the MetaGOflow version stanza")
        sys.exit()

    # Add ena_accession_number to the "identifier" field
    for stanza in template["@graph"]:
        # Not yet formatted
        if stanza["@id"] == "{ena_accession_number_url}":
            stanza["@id"] = stanza["@id"].format(**conf)
            stanza["name"] = stanza["name"].format(**conf)
            stanza["downloadUrl"] = stanza["downloadUrl"].format(**conf)
            break
    else:
        log.error("Cannot find the ENA accession number stanza")
        sys.exit()

    # Now add structural elements
    # wasAsscoicatedWith - the sampling event persons and institution
    # "wasAssociatedWith": {}
    template["@graph"][1]["wasAssociatedWith"] = template["@graph"][1][
        "wasAssociatedWith"
    ] = dict([("@id", f"{conf['sampling_person_name']}")])

    # creator  - the MGF data creator and institution
    # "creator": {}
    template["@graph"][1]["creator"] = template["@graph"][1]["creator"] = dict(
        [("@id", f"{conf['creator_person_name']}")]
    )

    # Add the sampling persons and institution stanzas
    for person in ["sampling_person", "creator_person"]:
        person_stanza = dict(
            [
                ("@id", f"{conf[f'{person}_name']}"),
                ("@type", "Person"),
                ("name", f"{conf[f'{person}_name']}"),
                ("identifier", f"{conf[f'{person}_identifier']}"),
                (
                    "affiliation",
                    f"https://edmo.seadatanet.org/report/{conf[f'{person}_station_edmoid']}",
                ),
            ]
        )
        template["@graph"].insert(5, person_stanza)

    # Add the sampling persons and institution stanzas
    sampling_person_station_stanza = dict(
        [
            (
                "@id",
                f"https://edmo.seadatanet.org/report/{conf['sampling_person_station_edmoid']}",
            ),
            ("@type", "Organization"),
            ("name", f"{conf['sampling_person_station_name']}"),
            ("country", f"{conf['samling_person_station_country']}"),
        ]
    )
    template["@graph"].insert(6, sampling_person_station_stanza)
    # Add creator institution if different from sampling institution
    if not conf["sampling_person_station_name"] == conf["creator_person_station_name"]:
        creator_person_station_stanza = dict(
            [
                (
                    "@id",
                    f"https://edmo.seadatanet.org/report/{conf['creator_person_station_edmoid']}",
                ),
                ("@type", "Organization"),
                ("name", f"{conf['sampling_person_station_name']}"),
                ("country", f"{conf['samling_person_station_country']}"),
            ]
        )
        template["@graph"].insert(8, creator_person_station_stanza)

    # Add sequence_categorisation stanza separately as they can vary in number and identity
    template, seq_cat_files = sequence_categorisation_stanzas(
        target_directory, template
    )

    # Format the {prefix} in the filepaths and other @id fields
    for stanza in template["@graph"]:
        stanza["@id"] = stanza["@id"].format(**conf)
        if "hasPart" in stanza:
            for entry in stanza["hasPart"]:
                entry["@id"] = entry["@id"].format(**conf)

    # Add the download URLs to mandatory files
    for filepath in filepaths:
        bits = Path(filepath).parts
        if len(bits) == 1:
            # "fastp.html",
            # "final.contigs.fa",
            # "RNA-counts",
            # "{run_parameter}.yml"
            filename = bits[0].format(**conf)
            for stanza in template["@graph"]:
                if stanza["@id"] == filename:
                    link = os.path.join(S3_STORE_URL, conf["run_id"] + "%2F" + filename)
                    stanza["downloadUrl"] = f"{link}"
        elif len(bits) == 2:
            # "functional-annotation/{prefix}.merged_CDS.I5.tsv.gz",
            # "functional-annotation/{prefix}.merged.hmm.tsv.gz",
            # "functional-annotation/{prefix}.merged.summary.go",
            # "functional-annotation/{prefix}.merged.summary.go_slim",
            # "functional-annotation/{prefix}.merged.summary.ips",
            # "functional-annotation/{prefix}.merged.summary.ko",
            # "functional-annotation/{prefix}.merged.summary.pfam",
            filename = bits[1].format(**conf)
            for stanza in template["@graph"]:
                if stanza["@id"] == filename:
                    link = os.path.join(
                        S3_STORE_URL,
                        conf["run_id"] + "%2F" + bits[0] + "%2F" + filename,
                    )
                    stanza["downloadUrl"] = f"{link}"
        elif len(bits) == 3:
            # "functional-annotation/stats/go.stats",
            # "functional-annotation/stats/interproscan.stats",
            # "functional-annotation/stats/ko.stats",
            # "functional-annotation/stats/orf.stats",
            # "functional-annotation/stats/pfam.stats",
            # "taxonomy-summary/LSU/krona.html",
            # "taxonomy-summary/SSU/krona.html",
            # "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq.gz",
            # "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq_hdf5.biom",
            # "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq_json.biom",
            # "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq.tsv",
            # "taxonomy-summary/SSU/{prefix}.merged_SSU.fasta.mseq.txt",
            # "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq.gz",
            # "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq_hdf5.biom",
            # "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq_json.biom",
            # "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq.tsv",
            # "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq.txt",
            filename = bits[-1].format(**conf)
            for stanza in template["@graph"]:
                if stanza["@id"] == filename:
                    link = os.path.join(
                        S3_STORE_URL,
                        conf["run_id"]
                        + "%2F"
                        + bits[0]
                        + "%2F"
                        + bits[1]
                        + "%2F"
                        + filename,
                    )
                    stanza["downloadUrl"] = f"{link}"
        else:
            log.error(f"Cannot find stanza for {filename}")
            sys.exit()

        log.debug(f"bits = {bits}")
        log.debug(f"filename = {filename}")
        log.debug(f"link = {link}")

    log.info("Metadata JSON formatted")
    return json.dumps(template, indent=4)


def main(target_directory, yaml_config, with_payload, debug):
    # Logging
    if debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO
    log.basicConfig(format="\t%(levelname)s: %(message)s", level=log_level)

    # Check the data directory name
    run_id = os.path.split(target_directory)[1]
    # if "." in data_dir:
    #    log.error(
    #        f"The target data directory ({data_dir}) cannot have a '.' period in it!"
    #    )
    #    log.error("Change it to '-' and try again")
    #    log.error("Bailing...")
    #    sys.exit()

    # Read the YAML configuration
    log.info("Reading YAML configuration...")
    conf = read_yaml(yaml_config)
    conf["run_id"] = run_id

    # Get the emo bon ref_code, batch number, and prefix
    conf = get_ref_code_and_prefix(conf)

    # Check all files are present
    log.info("Checking data files...")
    filepaths = check_and_format_data_file_paths(
        target_directory, conf, check_exists=True
    )

    # Write the ro-crate-metadata.json file
    log.info("Writing metadata.json...")
    metadata_json_formatted = write_metadata_json(target_directory, conf, filepaths)
    with open("ro-crate-metadata.json", "w") as outfile:
        outfile.write(metadata_json_formatted)
    writeHTMLpreview()
    # print("Written %s" % metadata_json_formatted)

    log.debug("with_payload = %s" % with_payload)
    if with_payload:
        # OK, all's good, let's build the RO-Crate
        log.info("Creating payload: copying data files...")
        with tempfile.TemporaryDirectory() as tmpdirname:
            # Deal with the YAML file
            yf = WORKFLOW_YAML_FILENAME.format(**conf)
            source = os.path.join(target_directory, yf)
            shutil.copy(source, os.path.join(tmpdirname, yf))

            # Build the ro-crate dir structure
            output_dirs = [
                "functional-annotation/stats",
                "sequence-categorisation",
                "taxonomy-summary/LSU",
                "taxonomy-summary/SSU",
            ]
            for d in output_dirs:
                os.makedirs(os.path.join(tmpdirname, d))
            # Loop over results files and sequence categorisation files
            for fp in filepaths:
                source = os.path.join(target_directory, "results", fp)
                log.debug("source = %s" % source)
                log.debug("dest = %s" % os.path.join(tmpdirname, fp))
                shutil.copy(source, os.path.join(tmpdirname, fp))

            # Write the json metadata file:
            with open(
                os.path.join(tmpdirname, "ro-crate-metadata.json"), "w"
            ) as outfile:
                outfile.write(metadata_json_formatted)

            # Write the HTML preview file
            writeHTMLpreview(tmpdirname)

            # Zip it up:
            log.info("Zipping data to ro-crate... (this could take some time...)")
            ro_crate_name = "%s-ro-crate" % os.path.split(target_directory)[1]
            shutil.make_archive(ro_crate_name, "zip", tmpdirname)
            log.info("Done")
    log.info("Build completed without error")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )
    parser.add_argument(
        "target_directory",
        help="Name of target directory containing MetaGOflow output",
    )
    parser.add_argument(
        "yaml_config", help="Name of YAML config file for building RO-Crate"
    )
    parser.add_argument(
        "-w",
        "--with_payload",
        help="Build the RO-Crate with the payload (data files)",
        default=False,
        action="store_true",
    )
    parser.add_argument("-d", "--debug", action="store_true", help="DEBUG logging")
    args = parser.parse_args()
    main(args.target_directory, args.yaml_config, args.with_payload, args.debug)
