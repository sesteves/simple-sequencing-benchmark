#!/usr/bin/env bash
javac -d out/ -cp lib/*:../cache-mining/lib/*:out/:. src/pt/inescid/gsd/ssb/*
scalac -d out src/*.scala
