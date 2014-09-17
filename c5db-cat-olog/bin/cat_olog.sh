#!/bin/sh
set -e

CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "$0" )" && pwd )"
C5_PARENT_DIR=${SCRIPT_DIR}/../..
C5DB_JAR=${C5_PARENT_DIR}/c5db/target/c5db-0.1-SNAPSHOT-jar-with-dependencies.jar
CAT_OLOG_JAR=${C5_PARENT_DIR}/c5db-cat-olog/target/c5db-cat-olog-0.1-SNAPSHOT.jar

if [ -f ${C5DB_JAR} ]
then
	echo "skipping c5db assembly"
else
	cd ${C5_PARENT_DIR}
	mvn -pl c5db assembly:single
fi

if [ -f ${CAT_OLOG_JAR} ]
then
	echo "skipping cat-olog"
else
	cd ${C5_PARENT_DIR}
	mvn -pl c5db-cat-olog install
fi

cd ${CURRENT_DIR}
java -cp ${C5DB_JAR}:${CAT_OLOG_JAR} c5db.log.CatOLog $1
