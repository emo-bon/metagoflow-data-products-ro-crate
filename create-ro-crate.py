#! /usr/bin/env python3

import os
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

# This is the workflow YAML file, the prefix is the "-n" parameter of the
# "run_wf.sh" script:
WORKFLOW_YAML_FILENAME = "{run_parameter}.yml"
CONFIG_YAML_PARAMETERS = [
    "datePublished",
    #    "prefix",
    "run_parameter",
    "ena_accession_raw_data",
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

PERSON_TEMPLATE = """
    "@id": "#{name}",
    "@type": "Person",
    "name": "{name}",
    "identifier": "{sampl_person_orcid}",
    "affiliation": {affiliation}
"""

STATION_TEMPLATE = """
    "@id": "{affiliation}",
    "@type": "Organization",
    "name": "{observatory_name}"
"""

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


def get_ref_code_and_prefix(run_id):
    """Get the reference code for a given run_id.
    run_id is the last part of the reads_name in the run information file.
    e.g. 'DBH_AAAAOSDA_1_HWLTKDRXY.UDI235' and is the name used to label the target_directory.
    """

    for batch in [BATCH1_RUN_INFO_PATH, BATCH2_RUN_INFO_PATH]:
        df = pd.read_csv(batch)
        for row in df[["reads_name", "ref_code"]].values.tolist():
            if row[0].split("_")[-1] == run_id:
                ref_code = row[1]
                prefix = row[0].split("_")[0]
                return ref_code, prefix
    return None


def writeHTMLpreview(tmpdirname):
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
            Path(tmpdirname, "ro-crate-metadata.json"),
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
    search = Path(target_directory, "results", "sequence-categorisation", "*.gz")
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
        d = dict(
            [("@id", fn), ("@type", "File"), ("encodingFormat", "application/zip")]
        )
        template["@graph"].insert(sq_index + 1, d)
    return template, seq_cat_files


def read_yaml(yaml_config):
    # Read the YAML configuration
    if not os.path.exists(yaml_config):
        log.error(f"YAML configuration file does not exist at {yaml_config}")
        log.error("Bailing...")
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


def check_and_format_data_file_paths(target_directory, conf):
    """Check that all mandatory files are present in the target directory"""

    # The workflow run YAML - lives in the toplevel dir not /results
    filename = WORKFLOW_YAML_FILENAME.format(**conf)
    path = Path(target_directory, filename)
    if not os.path.exists(path):
        log.error(YAML_ERROR)
        sys.exit()

    # format the filepaths:
    filepaths = [f.format(**conf) for f in MANDATORY_FILES]
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
            log.error("Bailing...")
            sys.exit()

    log.info("Data look good...")
    return filepaths


def get_persons_and_inst_stanzas(ref_code):
    """Get the sampling person and institution for a given ref_code.

    This is pretty fragile code...
    TODO: refactor this to be more robust and write some unit tests
    """

    conf = {}
    # Read the relevant rows in sample and observatory sheets
    df_samp = pd.read_csv(COMBINED_LOGSHEETS_PATH)
    row_samp = df_samp.loc[df_samp["ref_code"] == ref_code].to_dict()
    df_obs = pd.read_csv(OBSERVATORY_LOGSHEETS_PATH)
    obs_id = list(row_samp["obs_id"].values())[0]
    env_package = list(row_samp["env_package"].values())[0]
    row_obs = df_obs.loc[
        (df_obs["obs_id"] == obs_id) & (df_obs["env_package"] == env_package)
    ].to_dict()
    # Sampling person stanza
    conf["name"] = list(row_samp["sampl_person"].values())[0]
    conf["sampl_person_orcid"] = list(row_samp["sampl_person_orcid"].values())[0]
    # Sampling person affiliation
    organization_edmoid = list(row_obs["organization_edmoid"].values())[0]
    aff = f"https://edmo.seadatanet.org/report/{organization_edmoid}"
    conf["affiliation"] = '{"@id": "%s"}' % aff
    conf["observatory_name"] = list(row_obs["organization"].values())[0]
    person_stanzas = "{\t%s}," % PERSON_TEMPLATE.format(**conf)
    station_stanzas = "{\t%s}," % STATION_TEMPLATE.format(**conf)

    # Format the wasAssociatedWith field
    awp = '"@id": "#{name}"'.format(**conf)
    associated_with_person = "{%s}" % awp

    # Deal with potentially a list of other_persons
    others = list(row_samp["other_person"].values())
    # We assume the other person is at the same station
    for i, value in enumerate(others):
        conf["name"] = value
        conf["sampl_person_orcid"] = list(row_samp["other_person_orcid"].values())[i]
        new_person = "\n{\t%s}," % PERSON_TEMPLATE.format(**conf)
        person_stanzas = person_stanzas + new_person
        np = '", "@id": "#{name}"'.format(**conf)
        associated_with_person = associated_with_person.replace('"}', np + "}")

    return associated_with_person, person_stanzas, station_stanzas


def write_metadata_json(target_directory, conf, filepaths):
    metadata_json_template = "ro-crate-metadata.json-template"
    if os.path.exists(metadata_json_template):
        with open(metadata_json_template, "r") as f:
            template = json.load(f)
    else:
        # Grab the template from Github
        req = requests.get(TEMPLATE_URL)
        if req.status_code == requests.codes.ok:
            template = req.json()
        else:
            log.error("Unable to download the metadata.json file from Github")
            log.error(f"Check {TEMPLATE_URL}")
            log.error("Bailing...")
            sys.exit()

    log.info("Writing ro-crate-metadata.json...")
    # Deal with the ./ dataset stanza separately
    # "ref_code"'s
    template["@graph"][1]["name"] = template["@graph"][1]["name"].format(**conf)
    template["@graph"][1]["description"] = template["@graph"][1]["description"].format(
        **conf
    )
    template["@graph"][1]["title"] = template["@graph"][1]["title"].format(**conf)
    # "datePublished"
    if "datePublished" in conf:
        template["@graph"][1]["datePublished"] = template["@graph"][1][
            "datePublished"
        ].format(**conf)
    else:
        template["@graph"][1]["datePublished"] = datetime.datetime.now().strftime(
            "%Y-%m-%d"
        )

    # Build the persons and institution stanzas
    associated_with_person, person_stanzas, station_stanzas = (
        get_persons_and_inst_stanzas(conf["ref_code"])
    )
    conf.update(
        {
            "associated_with_person": associated_with_person,
            "person_stanzas": person_stanzas,
            "station_stanzas": station_stanzas,
        }
    )
    # wasAsscoicatedWith - the sampling event persons and institution
    template["@graph"][1]["wasAssociatedWith"] = template["@graph"][1][
        "wasAssociatedWith"
    ].format(**conf)

    # creator - the person who ran the workflow and their institution
    template["@graph"][1]["creator"] = template["@graph"][1]["creator"].format(**conf)

    # deal with sequence_categorisation separately
    template, seq_cat_files = sequence_categorisation_stanzas(
        target_directory, template
    )
    # add seq cat files to the filepaths
    for scf in seq_cat_files:
        filepaths.append(os.path.join("sequence-categorisation", scf))
    ### deal with the rest
    for section in template["@graph"]:
        section["@id"] = section["@id"].format(**conf)
        if "hasPart" in section:
            for entry in section["hasPart"]:
                entry["@id"] = entry["@id"].format(**conf)

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
    data_dir = os.path.split(target_directory)[1]
    if "." in data_dir:
        log.error(
            f"The target data directory ({data_dir}) cannot have a '.' period in it!"
        )
        log.error("Change it to '-' and try again")
        log.error("Bailing...")
        sys.exit()

    # Read the YAML configuration
    log.info("Reading YAML configuration...")
    conf = read_yaml(yaml_config)

    # Get the emo bon ref_code
    ref_code, prefix = get_ref_code_and_prefix(target_directory)
    if not ref_code:
        log.error("Could not find the ref_code for this run")
        sys.exit()
    else:
        conf["ref_code"] = ref_code
        conf["prefix"] = prefix

    # Check all files are present
    log.info("Checking data files...")
    filepaths = check_and_format_data_file_paths(target_directory, conf)

    # Write the ro-crate-metadata.json file
    log.info("Writing metadata.json...")
    metadata_json_formatted = write_metadata_json(target_directory, conf, filepaths)

    if not with_payload:
        # Debug to disk
        with open("ro-crate-metadata.json", "w") as outfile:
            outfile.write(metadata_json_formatted)
        log.debug("Written %s" % metadata_json_formatted)
    else:
        # OK, all's good, let's build the RO-Crate
        log.info("Copying data files...")
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
        "with_payload",
        help="Build the RO-Crate with the payload (data files)",
        default=False,
        action="store_true",
    )
    parser.add_argument("-d", "--debug", action="store_true", help="DEBUG logging")
    args = parser.parse_args()
    main(args.target_directory, args.yaml_config, args.with_payload, args.debug)
