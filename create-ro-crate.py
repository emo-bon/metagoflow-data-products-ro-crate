#! /usr/bin/env python3

desc = """
Build a MetaGOflow Data Products ro-crate from a YAML configuration.

Invoke
$ create-ro-crate.py <target_directory> <yaml_configuration>

where:
    target_directory is the toplevel output directory of MetaGOflow
        Note that the name of the directory cannot have a "." in it!
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

import os
import argparse
import textwrap
import sys
import yaml
import json
import datetime
import base64
import requests
import tempfile
import shutil
import glob
import subprocess
import logging as log

#This is the workflow YAML file, the prefix is the "-n" parameter of the
#"run_wf.sh" script:
yaml_file = "{run_parameter}.yml"
#InterProScan file(s) have to be dealt with separately until the wf is fixed
interproscan_file = "{prefix}.merged_CDS.I5.tsv.gz"
yaml_parameters = ["datePublished", "prefix", "run_parameter",
    "ena_accession_raw_data", "metagoflow_version", "missing_files"]

mandatory_files = [
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
    "taxonomy-summary/LSU/{prefix}.merged_LSU.fasta.mseq.txt"
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

def writeHTMLpreview(tmpdirname):
    """Write the HTML preview file using rochtml-
    https://www.npmjs.com/package/ro-crate-html
    """
    rochtml_path = shutil.which("rochtml")
    if not rochtml_path:
       log.info("HTML preview file cannot be written due to missing executable (rochtml)")
    else:
        cmd = "%s %s" % (rochtml_path, os.path.join(tmpdirname, "ro-crate-metadata.json"))
        child = subprocess.Popen(str(cmd), stdout=subprocess.PIPE,
             stderr=subprocess.PIPE, shell=True)
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

def joinInterProScanOutputFiles(target_directory, conf):
    """IPS outputs one, or many, files (sigh)

    {prefix}.merged_CDS.I5.tsv.gz
    or
    DBB.merged_CDS.I5_00{1-9}.tsv.gz

    An issue has been raised to fix the workflow, but for the
    time being we are going to cat them here
    """
    log.info("Cat'ing the IPS chunk files... (this could take some time...)")
    s = "{prefix}.merged_CDS.I5*tsv.gz".format(**conf)
    path = os.path.join(target_directory, "results", "functional-annotation", s)
    r = glob.glob(path)
    if len(r) < 2:
        log.error("Unable to locate 2 or more InterProScan files")
        log.error("They should be of the form: {prefix}.merged_CDS.I5_00{1-9}.tsv.gz")
        log.error("Bailing...")
        sys.exit()
    #cat the chunks together
    outfile = os.path.join(target_directory, "results", "functional-annotation",
            "{prefix}.merged_CDS.I5.tsv.gz".format(**conf))
    cmd = "cat %s > %s" % (" ".join(r), outfile)
    child = subprocess.Popen(str(cmd), stdout=subprocess.PIPE,
         stderr=subprocess.PIPE, shell=True)
    stdoutdata, stderrdata = child.communicate()
    return_code = child.returncode
    if return_code != 0:
        log.error("Error whilst trying to concatenate IPS files")
        log.debug("Stderr: %s " % stderrdata)
        log.debug("Files = %s" % r)
        log.debug("Command: %s" % cmd)
        log.debug("Return code: %s" % return_code)
        log.error("Bailing...")
        sys.exit()

def sequence_categorisation_stanzas(target_directory, template):
    """Glob the sequence_categorisation directory and build a stanza for each
    zipped data file

    Return updated template, and list of sequence category filenames
    """
    search = os.path.join(target_directory, "results", "sequence-categorisation", "*.gz")
    seq_cat_paths = glob.glob(search)
    seq_cat_files = [os.path.split(f)[1] for f in seq_cat_paths]
    #Sequence-categorisation stanza
    for i, stanza in enumerate(template["@graph"]):
        if stanza["@id"] == "sequence-categorisation/":
            stanza["hasPart"] = [dict([("@id", fn)]) for fn in
                    seq_cat_files]
            sq_index = i
            break

    seq_cat_files.reverse()
    for fn in seq_cat_files:
        d = dict([("@id", fn), ("@type", "File"),
            ("encodingFormat", "application/zip")])
        template["@graph"].insert(sq_index+1, d)
    return template, seq_cat_files

def main(target_directory, yaml_config, debug):

    #Logging
    if debug:
        log_level = log.DEBUG
    else:
        log_level = log.INFO
    log.basicConfig(format='\t%(levelname)s: %(message)s', level=log_level)

    #Check the data directory name
    data_dir = os.path.split(target_directory)[1]
    if "." in data_dir:
        log.error(f"The target data directory ({data_dir}) cannot have a '.' period in it!")
        log.error("Change it to '-' and try again")
        log.error("Bailing...")
        sys.exit()

    #Read the YAML configuration
    if not os.path.exists(yaml_config):
        log.error(f"YAML configuration file does not exist at {yaml_config}")
        log.error("Bailing...")
        sys.exit()
    with open(yaml_config, "r") as f:
        conf = yaml.safe_load(f)
    #Check yaml parameters are formated correctly, but not necessarily sane
    for param in yaml_parameters:
        log.debug("Config paramater: %s" % conf[param])
        if param == "datePublished":
            if conf[param] == "None":
                #No specified date, delete from conf
                #Its absence will trigger formatting
                #with today's date
                del conf[param]
                continue
            else:
                if not isinstance(conf[param], str):
                    log.error("'dataPublished' should either be a string or 'None'. Bailing...")
                    sys.exit()
                try:
                    datetime.datetime.fromisoformat(conf[param])
                except ValueError:
                    log.error(f"'datePublished' must conform to ISO 8601: {param}")
                    log.error("Bailing...")
                    sys.exit()
        elif param == "missing_files":
            if not param in conf:
                continue
            else:
                for filename in conf[param]:
                    if not isinstance(filename, str):
                        log.error(f"Parameter '{filename}' in 'missing_files' list in YAML file must be a string.")
                        log.error("Bailing...")
                        sys.exit()
        else:
            if not conf[param] or not isinstance(conf[param], str):
                log.error(f"Parameter '{param}' in YAML file must be a string.")
                log.error("Bailing...")
                sys.exit()

    #Check all files are present

    #The workflow run YAML - lives in the toplevel dir not /results
    filename = yaml_file.format(**conf)
    path = os.path.join(target_directory, filename)
    if not os.path.exists(path):
        log.error(YAML_ERROR)
        sys.exit()

    #format the filepaths:
    filepaths = [f.format(**conf) for f in mandatory_files]
    #The fixed file paths
    for filepath in filepaths:
        log.debug(f"File path: {filepath}")
        path = os.path.join(target_directory, "results", filepath)
        if not os.path.exists(path):
            if "missing_files" in conf:
                if os.path.split(filepath)[1] in conf["missing_files"]:
                    #This file is known to be missing, ignoring
                    log.info("Ignoring specified missing file: %s" %
                            os.path.split(filepath)[1])
                    filepaths.remove(filepath)
                    continue
            log.error("Could not find the mandatory file '%s' at the following path: %s" %
                        (filepath, path))
            log.error("Consider adding it to the 'missing_files' list in the YAML configuration.")
            log.error("Bailing...")
            sys.exit()

    ### if the IPS files are split, join them
    ipsf = os.path.join("functional-annotation",
            "{prefix}.merged_CDS.I5.tsv.gz".format(**conf))
    ipsf_path = os.path.join(target_directory, "results", ipsf)
    if not os.path.exists(ipsf_path):
        ### deal with split DBB.merged_CDS.I5_001.tsv.gz files
        joinInterProScanOutputFiles(target_directory, conf)
    filepaths.append(ipsf)

    log.info("Data look good...")

    #Let's deal with the JSON metadata file
    # Grab the template from Github
    # https://stackoverflow.com/questions/38491722/reading-a-github-file-using-python-returns-html-tags
    url = "https://raw.githubusercontent.com/emo-bon/MetaGOflow-Data-Products-RO-Crate/main/ro-crate-metadata.json-template"
    req = requests.get(url)
    if req.status_code == requests.codes.ok:
        template = req.json()
    else:
        log.error("Unable to download the metadata.json file from Github")
        log.error(f"Check {url}")
        log.error("Bailing...")
        sys.exit()

    #Metadata template on disk
    #metadata_json_template = "ro-crate-metadata.json-template"
    #with open(metadata_json_template, "r") as f:
    #   template = json.load(f)

    log.info("Writing ro-crate-metadata.json...")
    #Deal with the ./ dataset stanza separately
    #"accession_number"
    template["@graph"][1]["name"] = template["@graph"][1]["name"].format(**conf)
    template["@graph"][1]["description"] = template["@graph"][1]["description"].format(**conf)
    #"datePublished"
    if "datePublished" in conf:
        template["@graph"][1]["datePublished"] = template["@graph"][1]["datePublished"].format(**conf)
    else:
        template["@graph"][1]["datePublished"] = datetime.datetime.now().strftime('%Y-%m-%d')

    # deal with sequence_categorisation separately
    template, seq_cat_files  = sequence_categorisation_stanzas(target_directory, template)
    # add seq cat files to the filepaths
    for scf in seq_cat_files:
        filepaths.append(os.path.join("sequence-categorisation", scf))
    ### deal with the rest
    for section in template["@graph"]:
        section["@id"] = section["@id"].format(**conf)
        if "hasPart" in section:
            for entry in section["hasPart"]:
                entry["@id"] = entry["@id"].format(**conf)
    #Write the json metadata file:
    metadata_json_formatted = json.dumps(template, indent=4)

    #Debug to disk
    #with open("testing-ro-crate-metadata.json", "w") as outfile:
    #    outfile.write(metadata_json_formatted)
    #sys.exit()
    log.debug("%s" % metadata_json_formatted)

    #OK, all's good, let's build the RO-Crate
    log.info("Copying data files...")
    with tempfile.TemporaryDirectory() as tmpdirname:
        #Deal with the YAML file
        yf = yaml_file.format(**conf)
        source = os.path.join(target_directory, yf)
        shutil.copy(source, os.path.join(tmpdirname, yf))

        #Build the ro-crate dir structure
        output_dirs = ["functional-annotation/stats", "sequence-categorisation",
        "taxonomy-summary/LSU", "taxonomy-summary/SSU"]
        for d in output_dirs:
            os.makedirs(os.path.join(tmpdirname, d))
        #Loop over results files and sequence categorisation files
        for fp in filepaths:
            source = os.path.join(target_directory, "results", fp)
            log.debug("source = %s" % source)
            log.debug("dest = %s" % os.path.join(tmpdirname, fp))
            shutil.copy(source, os.path.join(tmpdirname, fp))

        #Write the json metadata file:
        with open(os.path.join(tmpdirname, "ro-crate-metadata.json"), "w") as outfile:
            outfile.write(metadata_json_formatted)

        #Write the HTML preview file
        writeHTMLpreview(tmpdirname)

        #Zip it up:
        log.info("Zipping data to ro-crate... (this could take some time...)")
        ro_crate_name = "%s-ro-crate" % os.path.split(target_directory)[1]
        shutil.make_archive(ro_crate_name, "zip", tmpdirname)
        log.info("done")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent(desc),
            )
    parser.add_argument("target_directory",
                        help="Name of target directory containing MetaGOflow" +\
                        "output"
                        )
    parser.add_argument("yaml_config",
                        help="Name of YAML config file for building RO-Crate"
                        )
    parser.add_argument('-d', '--debug',
                    action='store_true',
                    help="DEBUG logging")
    args = parser.parse_args()
    main(args.target_directory, args.yaml_config, args.debug)

