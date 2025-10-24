#! /usr/bin/env python3

desc = """
Convert MGF RO-Crate ro-crate-metadata.json files to RDF triples with correctly
qualified IRIs on GitHub
"""

import argparse
import pprint
import rdflib
import logging as log
import textwrap
from pathlib import Path

GITHUB_PREFIX_ADDRESS = "https://github.com/emo-bon/analysis-results-cluster-01-crate"
LOCAL_PREFIX_PATH = "file:///home/cymon/work/git-repos/metagoflow-data-products-ro-crate/analysis-results-cluster-01-crate" 


def main(path_to_crate, debug=False):


    log.basicConfig(
        format="\t%(levelname)s: %(message)s", level=log.DEBUG if debug else log.INFO
    )

    ro_path = Path(path_to_crate)
    ro_crate_name = ro_path.name
    outfile = Path(ro_path.parent, f"{ro_crate_name}.ttl")

    graph = rdflib.Graph()
    graph.parse(Path(ro_path, "ro-crate-metadata.json"))

    for triple in graph:

        log.debug(f"Triple {triple}")
        found = False
        new_triple = list(triple)

        # A hasPart triple has both Subject and Object with local paths
        if triple[0].startswith(LOCAL_PREFIX_PATH):
            subject = rdflib.URIRef(
                triple[0].replace(LOCAL_PREFIX_PATH, GITHUB_PREFIX_ADDRESS)
            )
            new_triple[0] = subject
            log.debug(f"New triple[0] = {new_triple}")
            found = True

        if triple[2].startswith(LOCAL_PREFIX_PATH):
            tobject = rdflib.URIRef(
                triple[2].replace(LOCAL_PREFIX_PATH, GITHUB_PREFIX_ADDRESS)
            )
            new_triple[2] = tobject
            log.debug(f"New triple[2] = {new_triple}")
            found = True
        if found:
            graph.remove(triple)
            graph.add(tuple(new_triple))

    graph.serialize(destination=outfile, format="turtle")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent(desc),
    )
    parser.add_argument(
        "target_directory",
        help=(
            "Name of target directory containing MetaGOflow RO-Crate"
            " and the ro-crate-metadata.json file "
        ),
    )
    parser.add_argument("-d", "--debug", action="store_true", help="DEBUG logging")
    args = parser.parse_args()
    main(
        args.target_directory, args.debug,
    )
