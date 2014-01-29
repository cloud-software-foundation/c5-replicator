#!/bin/sh
set -ex
mvn clean compile assembly:single
rm -rf zip
mkdir zip
cp target/*dep*.jar zip 
cp main.json zip
(cd zip && zip osv.zip main*.json  *dep*.jar; cp osv.zip ../target) 
