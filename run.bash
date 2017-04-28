#!/usr/bin/env bash

# benchmark params
app=benchmark
seqssize=1
seqtype='ROW'
seqminsize=3
seqmaxsize=20
blocksize=1000
zipfn=100
nops=1000

# cache params
enabled='true'
cachesize='1000'

fname=stats-all-$app-$enabled-$(date +%s).csv
header="seqssize,seqtype,seqminsize,seqmaxsize,blocksize,zipfn,zipfe,nops"


for zipfe in 3; do

  java -cp lib/*:../cache-mining/lib/*:resources/:out/:. -Denabled=$enabled -Dcache-size=$cachesize  pt.inescid.gsd.ssb.Main $seqssize $seqtype $seqminsize $seqmaxsize $blocksize $zipfn $zipfe $nops

  benchmarkfname=$(ls stats-benchmark-* | tail -n 1)
  cachefname=$(ls stats-cache-* | tail -n 1)

  cfg="$seqssize,$seqtype,$seqminsize,$seqmaxsize,$blocksize,$zipfn,$zipfe,$nops"
  scala Merge $header $cfg benchmarkfname cachefname

done