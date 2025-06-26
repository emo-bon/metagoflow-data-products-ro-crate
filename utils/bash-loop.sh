#! /usr/bin/env bash

# Delete objects in S3
#s5cmd --profile eosc-fairease1 --endpoint-url https://s3.mesocentre.uca.fr rm s3://mgf-data-products/*
# Remove previous ro-crate builds
#rm -rf metaGOflow-rocrates-dvc/*ro-crate

# Prepare FILTER archives
#./utils/prepare_data.py working/FILTERS
# Prepare SEDIMENT archives
#./utils/prepare_data.py working/SEDIMENTS


# Build FILTER ro-crates
for f in working/data/FILTERS/prepared_archives/*; do
    echo ./create-ro-crate.py -uo $f working/config.yaml;
    ./create-ro-crate.py -uo $f working/config.yaml;
done

# Build SEDIMENT ro-crates
for f in working/data/SEDIMENTS/prepared_archives/*; do
    echo ./create-ro-crate.py -uo $f working/config.yaml;
    ./create-ro-crate.py -uo $f working/config.yaml;
done

