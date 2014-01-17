#!/bin/sh

protoc --java_out=src/main/java -Isrc/main/resources src/main/resources/*.proto
