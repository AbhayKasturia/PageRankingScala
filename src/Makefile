#!/bin/bash
# Author: Nat Tuck

jar:
	sbt package
	cp target/scala-*/pagerank_*.jar PageRank.jar

run: jar
	rm -rf out
	sbt "run input output"

test: sbt package
	cp target/scala-*/pagerank_*.jar PageRank.jar
	rm -rf out
	sbt "run input output"

clean:
	rm -rf project output target *.jar out
	clear
