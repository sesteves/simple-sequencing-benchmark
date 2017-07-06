#!/usr/bin/env bash

app=benchmark
maxlength=15
maxgap=1
zipfe=(0.5 1.0 1.5 2.0 2.5 3)
count=0

fname=stats-mining-$app-$(date +%s).csv
header="zipfe,minsup,maxlength,maxgap,time,memory,sequences,fsequences"

echo $header >> fname

for f in *.txt; do
  for minsup in 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1 0.2 0.3 0.4 0.5; do
    java -jar spmf.jar run VMSP $f result $minsup $maxlength $maxgap > out

    time=$(grep -Po "(?<=Total time ~ )[[:digit:]]+(?= ms)" out)
    memory=$(grep -Po "(?<=Max memory \(mb\) : )[.[:digit:]]+" out)
    sequences=$(grep -Po "(?<=count : )[[:digit:]]+" out)
    fsequences=$(grep -E "(-.+-.+-)" result | wc -l)

    echo "${zipfe[count]},$minsup,$maxlength,$maxgap,$time,$memory,$sequences,$fsequences" >> fname
  done
  count=${count}+1
done