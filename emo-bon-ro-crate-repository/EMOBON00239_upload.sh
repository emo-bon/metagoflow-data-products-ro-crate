#!/bin/bash
set -e
set -x

dvc add EMOBON00239-ro-crate/fastp.html
dvc add EMOBON00239-ro-crate/final.contigs.fa
dvc add EMOBON00239-ro-crate/taxonomy-summary/RNA-counts
dvc add EMOBON00239-ro-crate/config.yml
dvc add EMOBON00239-ro-crate/functional-annotation/stats/go.stats
dvc add EMOBON00239-ro-crate/functional-annotation/stats/interproscan.stats
dvc add EMOBON00239-ro-crate/functional-annotation/stats/ko.stats
dvc add EMOBON00239-ro-crate/functional-annotation/stats/orf.stats
dvc add EMOBON00239-ro-crate/functional-annotation/stats/pfam.stats
dvc add EMOBON00239-ro-crate/taxonomy-summary/LSU/krona.html
dvc add EMOBON00239-ro-crate/taxonomy-summary/SSU/krona.html
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged_CDS.I5.tsv.gz
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged.hmm.tsv.gz
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.go
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.go_slim
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.ips
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.ko
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.pfam
dvc add EMOBON00239-ro-crate/functional-annotation/DBH.merged.emapper.summary.eggnog
dvc add EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.gz
dvc add EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_hdf5.biom
dvc add EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_json.biom
dvc add EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.tsv
dvc add EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.txt
dvc add EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.gz
dvc add EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_hdf5.biom
dvc add EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_json.biom
dvc add EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.tsv
dvc add EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.txt
dvc add EMOBON00239-ro-crate/run.yml
dvc add EMOBON00239-ro-crate/sequence-categorisation/SSU_rRNA_archaea.RF01959.fa.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/LSU_rRNA_eukarya.RF02543.fa.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/5_8S.fa.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/LSU_rRNA_bacteria.RF02541.fa.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/SSU_rRNA_bacteria.RF00177.fa.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/tmRNA.RF00023.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/Archaea_SRP.RF01857.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/Bacteria_small_SRP.RF00169.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/Bacteria_large_SRP.RF01854.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/tRNA-Sec.RF01852.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/tRNA.RF00005.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/SSU_rRNA_eukarya.RF01960.fa.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/alpha_tmRNA.RF01849.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/RNaseP_bact_a.RF00010.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/Metazoa_SRP.RF00017.fasta.gz
dvc add EMOBON00239-ro-crate/sequence-categorisation/LSU_rRNA_archaea.RF02540.fa.gz

rm EMOBON00239-ro-crate/fastp.html
rm EMOBON00239-ro-crate/final.contigs.fa
rm EMOBON00239-ro-crate/taxonomy-summary/RNA-counts
rm EMOBON00239-ro-crate/config.yml
rm EMOBON00239-ro-crate/functional-annotation/stats/go.stats
rm EMOBON00239-ro-crate/functional-annotation/stats/interproscan.stats
rm EMOBON00239-ro-crate/functional-annotation/stats/ko.stats
rm EMOBON00239-ro-crate/functional-annotation/stats/orf.stats
rm EMOBON00239-ro-crate/functional-annotation/stats/pfam.stats
rm EMOBON00239-ro-crate/taxonomy-summary/LSU/krona.html
rm EMOBON00239-ro-crate/taxonomy-summary/SSU/krona.html
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged_CDS.I5.tsv.gz
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged.hmm.tsv.gz
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.go
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.go_slim
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.ips
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.ko
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged.summary.pfam
rm EMOBON00239-ro-crate/functional-annotation/DBH.merged.emapper.summary.eggnog
rm EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.gz
rm EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_hdf5.biom
rm EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_json.biom
rm EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.tsv
rm EMOBON00239-ro-crate/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.txt
rm EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.gz
rm EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_hdf5.biom
rm EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_json.biom
rm EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.tsv
rm EMOBON00239-ro-crate/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.txt
rm EMOBON00239-ro-crate/run.yml
rm EMOBON00239-ro-crate/sequence-categorisation/SSU_rRNA_archaea.RF01959.fa.gz
rm EMOBON00239-ro-crate/sequence-categorisation/LSU_rRNA_eukarya.RF02543.fa.gz
rm EMOBON00239-ro-crate/sequence-categorisation/5_8S.fa.gz
rm EMOBON00239-ro-crate/sequence-categorisation/LSU_rRNA_bacteria.RF02541.fa.gz
rm EMOBON00239-ro-crate/sequence-categorisation/SSU_rRNA_bacteria.RF00177.fa.gz
rm EMOBON00239-ro-crate/sequence-categorisation/tmRNA.RF00023.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/Archaea_SRP.RF01857.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/Bacteria_small_SRP.RF00169.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/Bacteria_large_SRP.RF01854.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/tRNA-Sec.RF01852.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/tRNA.RF00005.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/SSU_rRNA_eukarya.RF01960.fa.gz
rm EMOBON00239-ro-crate/sequence-categorisation/alpha_tmRNA.RF01849.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/RNaseP_bact_a.RF00010.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/Metazoa_SRP.RF00017.fasta.gz
rm EMOBON00239-ro-crate/sequence-categorisation/LSU_rRNA_archaea.RF02540.fa.gz

