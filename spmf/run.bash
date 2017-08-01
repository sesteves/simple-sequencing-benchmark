#!/usr/bin/env bash

app=benchmark
minlength=3
maxlength=15
maxgap=1
zipfe=(0.5 1.0 1.5 2.0 2.5 3)
count=0

fname=stats-mining-$app-$(date +%s).csv
header="algo,zipfe,minsup,maxlength,maxgap,time,memory,sequences,fsequences"

echo $header >> $fname

for f in *.txt; do
  for algo in GSP SPADE SPAM PrefixSpan ClaSP MaxSP VMSP VGEN; do
    echo "trying file $count and algorithm $algo..."

      for minsup in 0.01 0.02 0.03 0.04 0.05 0.06 0.07 0.08 0.09 0.1 0.2 0.3 0.4 0.5; do
        if [ $algo = VMSP ] || [ $algo = VGEN ] ; then
          java -jar spmf.jar run $algo $f result $minsup $maxlength $maxgap > out
        elif [ $algo = SPAM ] ; then
          java -jar spmf.jar run $algo $f result $minsup $minlength $maxlength $maxgap > out
        elif [ $algo = PrefixSpan ]; then
          java -jar spmf.jar run $algo $f result $minsup $maxlength > out
        else
          java -jar spmf.jar run $algo $f result $minsup > out
        fi

        time=$(grep -Po "(?<=Total time ~ )[[:digit:]]+(?= ms)" out)
        memory=$(grep -Po "Max memory \(mb\)\s?:\s?\K[[:digit:]]+" out)
        sequences=$(grep -Po "(?<=sequences count : )[[:digit:]]+" out)
        fsequences=$(grep -E "(-.+-.+-)" result | wc -l)

        echo "$algo,${zipfe[count]},$minsup,$maxlength,$maxgap,$time,$memory,$sequences,$fsequences" >> $fname
      done
    done
    count=$(($count + 1))
done