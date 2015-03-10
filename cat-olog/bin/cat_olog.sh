#!/bin/sh
set -e

CURRENT_DIR="$(pwd)"
BUILD_DIR="$( cd "$( dirname "$0" )" && pwd )"/../target
CAT_OLOG_CLASSPATH=${BUILD_DIR}/class-path.txt
CAT_OLOG_CLASSES=${BUILD_DIR}/classes

if [ ! -f ${CAT_OLOG_CLASSPATH} ]
then
	echo "The cat-olog classpath file was not found; please run 'mvn clean install' from the parent directory"
else
    java -cp `cat ${CAT_OLOG_CLASSPATH}`:${CAT_OLOG_CLASSES} c5db.log.CatOLog $1
fi
