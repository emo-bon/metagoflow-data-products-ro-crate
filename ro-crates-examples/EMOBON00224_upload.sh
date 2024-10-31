#!/bin/bash
set -e
set -x

mv HCFCYDSX5.UDI129 EMOBON00224

dvc add EMOBON00224/results/fastp.html
git add EMOBON00224/results/fastp.html.dvc
dvc add EMOBON00224/results/final.contigs.fa
git add EMOBON00224/results/final.contigs.fa.dvc
dvc add EMOBON00224/results/RNA-counts
git add EMOBON00224/results/RNA-counts.dvc
dvc add EMOBON00224/config.yml
git add EMOBON00224/config.yml.dvc
dvc add EMOBON00224/results/functional-annotation/stats/go.stats
git add EMOBON00224/results/functional-annotation/stats/go.stats.dvc
dvc add EMOBON00224/results/functional-annotation/stats/interproscan.stats
git add EMOBON00224/results/functional-annotation/stats/interproscan.stats.dvc
dvc add EMOBON00224/results/functional-annotation/stats/ko.stats
git add EMOBON00224/results/functional-annotation/stats/ko.stats.dvc
dvc add EMOBON00224/results/functional-annotation/stats/orf.stats
git add EMOBON00224/results/functional-annotation/stats/orf.stats.dvc
dvc add EMOBON00224/results/functional-annotation/stats/pfam.stats
git add EMOBON00224/results/functional-annotation/stats/pfam.stats.dvc
dvc add EMOBON00224/results/taxonomy-summary/LSU/krona.html
git add EMOBON00224/results/taxonomy-summary/LSU/krona.html.dvc
dvc add EMOBON00224/results/taxonomy-summary/SSU/krona.html
git add EMOBON00224/results/taxonomy-summary/SSU/krona.html.dvc
dvc add EMOBON00224/results/functional-annotation/DBH.merged_CDS.I5.tsv.gz
git add EMOBON00224/results/functional-annotation/DBH.merged_CDS.I5.tsv.gz.dvc
dvc add EMOBON00224/results/functional-annotation/DBH.merged.hmm.tsv.gz
git add EMOBON00224/results/functional-annotation/DBH.merged.hmm.tsv.gz.dvc
dvc add EMOBON00224/results/functional-annotation/DBH.merged.summary.go
git add EMOBON00224/results/functional-annotation/DBH.merged.summary.go.dvc
dvc add EMOBON00224/results/functional-annotation/DBH.merged.summary.go_slim
git add EMOBON00224/results/functional-annotation/DBH.merged.summary.go_slim.dvc
dvc add EMOBON00224/results/functional-annotation/DBH.merged.summary.ips
git add EMOBON00224/results/functional-annotation/DBH.merged.summary.ips.dvc
dvc add EMOBON00224/results/functional-annotation/DBH.merged.summary.ko
git add EMOBON00224/results/functional-annotation/DBH.merged.summary.ko.dvc
dvc add EMOBON00224/results/functional-annotation/DBH.merged.summary.pfam
git add EMOBON00224/results/functional-annotation/DBH.merged.summary.pfam.dvc
dvc add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.gz
git add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.gz.dvc
dvc add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_hdf5.biom
git add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_hdf5.biom.dvc
dvc add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_json.biom
git add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq_json.biom.dvc
dvc add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.tsv
git add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.tsv.dvc
dvc add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.txt
git add EMOBON00224/results/taxonomy-summary/SSU/DBH.merged_SSU.fasta.mseq.txt.dvc
dvc add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.gz
git add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.gz.dvc
dvc add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_hdf5.biom
git add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_hdf5.biom.dvc
dvc add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_json.biom
git add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq_json.biom.dvc
dvc add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.tsv
git add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.tsv.dvc
dvc add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.txt
git add EMOBON00224/results/taxonomy-summary/LSU/DBH.merged_LSU.fasta.mseq.txt.dvc
dvc add EMOBON00224/run.yml
git add EMOBON00224/run.yml.dvc
dvc add EMOBON00224/results/sequence-categorisation/Bacteria_small_SRP.RF00169.fasta.gz
git add EMOBON00224/results/sequence-categorisation/Bacteria_small_SRP.RF00169.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/tRNA.RF00005.fasta.gz
git add EMOBON00224/results/sequence-categorisation/tRNA.RF00005.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/Metazoa_SRP.RF00017.fasta.gz
git add EMOBON00224/results/sequence-categorisation/Metazoa_SRP.RF00017.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/RNaseP_nuc.RF00009.fasta.gz
git add EMOBON00224/results/sequence-categorisation/RNaseP_nuc.RF00009.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/LSU_rRNA_eukarya.RF02543.fa.gz
git add EMOBON00224/results/sequence-categorisation/LSU_rRNA_eukarya.RF02543.fa.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/tRNA-Sec.RF01852.fasta.gz
git add EMOBON00224/results/sequence-categorisation/tRNA-Sec.RF01852.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/LSU_rRNA_bacteria.RF02541.fa.gz
git add EMOBON00224/results/sequence-categorisation/LSU_rRNA_bacteria.RF02541.fa.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/SSU_rRNA_archaea.RF01959.fa.gz
git add EMOBON00224/results/sequence-categorisation/SSU_rRNA_archaea.RF01959.fa.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/5_8S.fa.gz
git add EMOBON00224/results/sequence-categorisation/5_8S.fa.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/Bacteria_large_SRP.RF01854.fasta.gz
git add EMOBON00224/results/sequence-categorisation/Bacteria_large_SRP.RF01854.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/SSU_rRNA_eukarya.RF01960.fa.gz
git add EMOBON00224/results/sequence-categorisation/SSU_rRNA_eukarya.RF01960.fa.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/RNaseP_bact_a.RF00010.fasta.gz
git add EMOBON00224/results/sequence-categorisation/RNaseP_bact_a.RF00010.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/SSU_rRNA_bacteria.RF00177.fa.gz
git add EMOBON00224/results/sequence-categorisation/SSU_rRNA_bacteria.RF00177.fa.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/alpha_tmRNA.RF01849.fasta.gz
git add EMOBON00224/results/sequence-categorisation/alpha_tmRNA.RF01849.fasta.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/LSU_rRNA_archaea.RF02540.fa.gz
git add EMOBON00224/results/sequence-categorisation/LSU_rRNA_archaea.RF02540.fa.gz.dvc
dvc add EMOBON00224/results/sequence-categorisation/tmRNA.RF00023.fasta.gz
git add EMOBON00224/results/sequence-categorisation/tmRNA.RF00023.fasta.gz.dvc

dvc push
git commit -m 'Added EMOBON00224 files'
git push

