#!/usr/bin/env bash
javac -d out/ -cp lib/*:../cache-mining/lib/*:out/:. src/pt/inescid/gsd/ssb/* src/pt/inescid/gsd/ssb/heuristics/*java
scalac -d out src/*.scala
