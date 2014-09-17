#!/bin/bash
set -ex

i=0
tests='echo starting'
for test in `ls c5*/pom.xml`
do
  if [ $(($i % $CIRCLE_NODE_TOTAL)) -eq $CIRCLE_NODE_INDEX ]
  then
    tests+=&& (cd `dirname ${test}` && mvn test)
  fi
  ((i++))|| ps ax
done

${tests}
