#!/bin/sh
set -ex
mvn test-compile failsafe:integration-test 
