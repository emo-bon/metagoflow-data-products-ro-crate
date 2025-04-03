cd ~/vscode/git-repos/metagoflow-data-products-ro-crate/working/arup-testing
cp ceta:/home/cymon/src/metaGOflow-cymon.git/FINISHED/Batch1and2/CCMAR-data/FILTERS/HVWGWDSX5.UDI134.tar.bz2 .
../../utils/prepare_data.py .
cd ../..
./create-ro-crate.py working/arup-testing/prepared_archives/HVWGWDSX5.UDI134 working/config.yaml -d
cd working/arup-testing
../../utils/arup_archive.py
apptainer run --bind :/rocrateroot ../../utils/emobon_arup.sif
