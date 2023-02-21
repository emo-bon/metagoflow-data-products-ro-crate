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
        https://github.com/emo-bon/MetaGOflow-Data-Products-RO-Crate/ro-crate-config.yaml

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

#This is the workflow YAML file, the prefix is the "-n" parameter of the
#"run_wf.sh" script:
yaml_file = "{run_parameter}.yml"
yaml_parameters = ["accession_number", "datePublished", "prefix",
        "run_parameter", "ena_accession_raw_data", "metagoflow_version"]

filepaths = [
    "fastp.html",
    "final.contigs.fa",
    "RNA-counts",
    "sequence-categorisation/5_8S.fa.gz",
    "sequence-categorisation/alpha_tmRNA.RF01849.fasta.gz",
    "sequence-categorisation/Bacteria_large_SRP.RF01854.fasta.gz",
    "sequence-categorisation/Bacteria_small_SRP.RF00169.fasta.gz",
    "sequence-categorisation/cyano_tmRNA.RF01851.fasta.gz",
    "sequence-categorisation/LSU.fasta.gz",
    "sequence-categorisation/LSU_rRNA_archaea.RF02540.fa.gz",
    "sequence-categorisation/LSU_rRNA_bacteria.RF02541.fa.gz",
    "sequence-categorisation/LSU_rRNA_eukarya.RF02543.fa.gz",
    "sequence-categorisation/Metazoa_SRP.RF00017.fasta.gz",
    "sequence-categorisation/Protozoa_SRP.RF01856.fasta.gz",
    "sequence-categorisation/RNase_MRP.RF00030.fasta.gz",
    "sequence-categorisation/RNaseP_bact_a.RF00010.fasta.gz",
    "sequence-categorisation/RNaseP_nuc.RF00009.fasta.gz",
    "sequence-categorisation/SSU.fasta.gz",
    "sequence-categorisation/SSU_rRNA_archaea.RF01959.fa.gz",
    "sequence-categorisation/SSU_rRNA_bacteria.RF00177.fa.gz",
    "sequence-categorisation/SSU_rRNA_eukarya.RF01960.fa.gz",
    "sequence-categorisation/tmRNA.RF00023.fasta.gz",
    "sequence-categorisation/tRNA.RF00005.fasta.gz",
    "sequence-categorisation/tRNA-Sec.RF01852.fasta.gz",
    "functional-annotation/stats/go.stats",
    "functional-annotation/stats/interproscan.stats",
    "functional-annotation/stats/ko.stats",
    "functional-annotation/stats/orf.stats",
    "functional-annotation/stats/pfam.stats",
    "taxonomy-summary/LSU/krona.html",
    "taxonomy-summary/SSU/krona.html"
    ]

prefix_filepaths = [
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

def main(target_directory, yaml_config):

    #Check the data directory name
    data_dir = os.path.split(target_directory)[1]
    if "." in data_dir:
        print("The target data directory (%s) cannot have a '.' period in it!")
        print("Change it to '-' and try again")
        print("Bailing...")
        sys.exit()

    #Read the YAML configuration
    if not os.path.exists(yaml_config):
        print(f"YAML configuration file does not exist at {yaml_config}")
        print("Bailing...")
        sys.exit()
    with open("test.yaml", "r") as f:
        conf = yaml.safe_load(f)
    #Check yaml parameters
    for param in yaml_parameters:
        if param == "datePublished":
            if conf[param] is False:
                continue
            else:
                if not isinstance(conf[param], str):
                    print("'dataPublished' should either be a string or False. Bailing...")
                    sys.exit()
        else:
            #print("%s" % conf[param])
            if not conf[param] or not isinstance(conf[param], str):
                print(f"Parameter '{param}' in YAML file must be a string.")
                print("Bailing...")
                sys.exit()

    #Check all files are present

    #The workflow run YAML - lives in the toplevel dir not /results
    filename = yaml_file.format(**conf)
    path = os.path.join(target_directory, filename)
    if not os.path.exists(path):
        print(YAML_ERROR)
        sys.exit()

    results_files = []
    #The fixed file paths
    for filepath in filepaths:
        path = os.path.join(target_directory, "results", filepath)
        if not os.path.exists(path):
            print("Could not find the file '%s' at the following path: %s" %
                    (filepath, path))
            print("Bailing...")
            sys.exit()
        else:
            results_files.append(filepath)

    #The prefixed file paths
    for ppath in prefix_filepaths:
        path = os.path.join(target_directory, "results", ppath.format(**conf))
        if not os.path.exists(path):
            print("Could not find the file '%s' at the following path: %s" %
                    (ppath, path))
            print("Bailing...")
            sys.exit()
        else:
            results_files.append(ppath.format(**conf))

    print("Data look good...")
    #Let's deal with the JSON metadata file
    # Grab the template from Github
    # https://stackoverflow.com/questions/38491722/reading-a-github-file-using-python-returns-html-tags
    url = "https://api.github.com/repos/emo-bon/MetaGOflow-Data-Products-RO-Crate/contents/ro-crate-metadata.json-template"
    req = requests.get(url)
    if req.status_code == requests.codes.ok:
        req = req.json()
        content = base64.b64decode(req['content'])
        template = json.load(content)
    else:
        print("Unable to download the metadata.json file from Github")
        print(f"Check {url}")
        print("Bailing...")
        sys.exit()

    #Metadata template on disk
    #metadata_json_template = "ro-crate-metadata.json-template" #!!!!!!!!!!  Delete when using Github
    #with open(metadata_json_template, "r") as f:
    #   template = json.load(f)

    print("Writing ro-crate-metadata.json...")
    #Deal with the ./ dataset stanza separately
    #"accession_number"
    template["@graph"][1]["name"] = template["@graph"][1]["name"].format(**conf)
    template["@graph"][1]["description"] = template["@graph"][1]["description"].format(**conf)
    #"datePublished"
    if "datePublished" in conf:
        template["@graph"][1]["datePublished"] = template["@graph"][1]["datePublished"].format(**conf)
    else:
        template["@graph"][1]["datePublished"] = datetime.datetime.now().strftime('%Y/%m/%d')
    #And the rest
    for section in template["@graph"]:
        section["@id"] = section["@id"].format(**conf)
        if "hasPart" in section:
            for entry in section["hasPart"]:
                entry["@id"] = entry["@id"].format(**conf)
    #Write the json metadata file:
    metadata_json_formatted = json.dumps(template, indent=4)

    #OK, all's good, let's build the RO-Crate
    print("Copying data files..."),
    with tempfile.TemporaryDirectory() as tmpdirname:
        #Deal with the YAML file
        yf = yaml_file.format(**conf)
        source = os.path.join(target_directory, yf)
        shutil.copy(source, os.path.join(tmpdirname, yf))
        #Results files
        for fp in results_files:
            #print("fp = %s" % fp)
            dest_dir = os.path.dirname(fp)
            #print("dest_dir = %s" % dest_dir)
            if dest_dir:
                tpath = os.path.join(tmpdirname, dest_dir)
                if not os.path.exists(tpath):
                    #print("dest_dir doesnt not exist")
                    path = os.path.join(tmpdirname, dest_dir)
                    #print("making dir at %s" % tpath)
                    os.makedirs(tpath)
            source = os.path.join(target_directory, "results", fp)
            #print("source = %s" % source)
            #print("dest = %s" % os.path.join(tmpdirname, fp))
            shutil.copy(source, os.path.join(tmpdirname, fp))
        #Write the json metadata file:
        with open(os.path.join(tmpdirname, "ro-crate-metadata.json"), "w") as outfile:
            outfile.write(metadata_json_formatted)

        #Zip it up:
        print("Zipping data to ro-crate...")
        ro_crate_name = "%s-ro-crate" % os.path.split(target_directory)[1]
        shutil.make_archive(ro_crate_name, "zip", tmpdirname)
        print("done")

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
    args = parser.parse_args()
    main(args.target_directory, args.yaml_config)

