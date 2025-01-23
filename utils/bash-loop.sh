#! /usr/bin/env bash
 
for f in working/ghost-archives/*; do
    echo ./create-ro-crate.py -d $f working/config.yaml;
    ./create-ro-crate.py -d $f working/config.yaml;
done
