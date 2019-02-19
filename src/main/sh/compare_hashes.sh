#!/bin/bash

set -e

if [[ -z "${SHARDMASTER_ZK_NODES}" ]]; then
    echo "Fatal error: Must provide SHARDMASTER_ZK_NODES in environment" 1>&2
    exit 1
fi

if [[ -z "${SHARDMASTER_ZK_PATH}" ]]; then
    echo "Fatal error: Must provide SHARDMASTER_ZK_PATH in environment" 1>&2
    exit 1
fi

if [[ -z "${IQL_URL}" ]]; then
    echo "Fatal error: Must provide IQL_URL in environment" 1>&2
    exit 1
fi

VERSION1=$1
VERSION2=$2

START_TIME="7d"
END_TIME="0d"

echo "Running comparator on versions $VERSION1 and $VERSION2 against time range $START_TIME to $END_TIME"
echo ""

WD=$(git rev-parse --show-toplevel)
TIMESTAMP=$(date +%Y%m%d.%H%M%S)

PARENT_DIR=/tmp/iql_cache_comparator_${TIMESTAMP}
mkdir "${PARENT_DIR}"

DIR1=${PARENT_DIR}/0
mkdir "${DIR1}"
DIR2=${PARENT_DIR}/1
mkdir "${DIR2}"

echo "Cloning and verifying compilation before doing work"
echo ""

OUTPUT_FILE=${PARENT_DIR}/main_output.txt

git clone --reference "${WD}" "${WD}" "${DIR1}" >> "${OUTPUT_FILE}"
git clone --reference "${WD}" "${WD}" "${DIR2}" >> "${OUTPUT_FILE}"

cd "${DIR1}"
git -c advice.detachedHead=false checkout "$1" >> "${OUTPUT_FILE}"

echo "Compiling version 1 ($VERSION1)"
echo ""
mvn clean compile >> "${OUTPUT_FILE}"

cd "${DIR2}"
git -c advice.detachedHead=false checkout "$2" >> "${OUTPUT_FILE}"

echo "Compiling version 2 ($VERSION2)"
echo ""
mvn clean compile >> "${OUTPUT_FILE}"

echo "Sampling queries"
echo ""
curl -G --compressed \
    "${IQL_URL}"\
     --data-urlencode "q=FROM iqlquery $START_TIME $END_TIME WHERE iqlversion=2 error=0 GROUP BY q" \
     --data-urlencode "client=iqlhashcomparator" \
     --data-urlencode "username=$USER" \
     --data-urlencode "csv=1" \
     --data-urlencode "v=2" \
     > "${PARENT_DIR}"/queries.csv

echo "Computing hashes"
echo ""

cd "${DIR1}"
mvn -q exec:java -Dexec.mainClass="com.indeed.iql2.HashQueries" -Dexec.args="${PARENT_DIR}/queries.csv ${DIR1}/hashes.csv" >> ${DIR1}/hash_stdout.txt &

# sleep for 1 second to allow indeed mvn wrapper to not throw an error about a symbolic
# link already existing
sleep 1s

cd "${DIR2}"
mvn -q exec:java -Dexec.mainClass="com.indeed.iql2.HashQueries" -Dexec.args="${PARENT_DIR}/queries.csv ${DIR2}/hashes.csv" >> ${DIR2}/hash_stdout.txt &

wait

echo "Comparing hashes"
echo ""

mvn -q exec:java -Dexec.mainClass="com.indeed.iql2.CompareHashes" -Dexec.args="${DIR1}/hashes.csv ${DIR2}/hashes.csv" | tee "${PARENT_DIR}"/cache_differences.txt