#!/usr/bin/env bash

# benchmark params
app=benchmark
seqssize=10
seqtype='ROW'
seqminsize=3
seqmaxsize=10
blocksize=1000
zipfn=1000
nops=1000

# cache params
cachesize='1000'

fname=stats-all-$app-$(date +%s).csv
header="enabled,seqssize,seqtype,seqminsize,seqmaxsize,blocksize,zipfn,zipfe,nops"
printheader=true


for enabled in 'false' 'true'; do
  for zipfe in 0.5 1.0 1.5 2.0 2.5 3; do
    if $enabled ; then
      for heuristic in 'fetch-all' 'fetch-top-n' 'fetch-progressively'; do
        echo "Executing... (enabled: true, zipfe: $zipfe, heuristic: $heuristic)"
        execute
      done
    else
      echo "Executing... (enabled: false, zipfe: $zipfe)"
      execute
    fi
  done
done

function execute {
  java -cp lib/*:../cache-mining/lib/*:resources/:out/:. -Denabled=$enabled -Dcache-size=$cachesize -Dheuristic=$heuristic pt.inescid.gsd.ssb.Benchmark $seqssize $seqtype $seqminsize $seqmaxsize $blocksize $zipfn $zipfe $nops

  benchmarkfname=$(ls stats-benchmark-* | tail -n 1)
  cachefname=$(ls stats-cache-* | tail -n 1)

  cfg="$enabled,$seqssize,$seqtype,$seqminsize,$seqmaxsize,$blocksize,$zipfn,$zipfe,$nops"
  scala -cp out Merge $header $printheader $cfg $benchmarkfname $cachefname >> $fname

  if $printheader ; then
    printheader=false
  fi
}
