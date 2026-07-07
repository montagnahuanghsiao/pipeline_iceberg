#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/load_runtime.sh"
: "${JAVA_HOME:?}" "${HADOOP_HOME:?}" "${HADOOP_CONF_DIR:?}"
: "${YARN_CONF_DIR:?}" "${SPARK_HOME:?}" "${ICEBERG_JAR:?}"
: "${ICEBERG_CATALOG:?}" "${ICEBERG_WAREHOUSE:?}"
: "${SPARK_EXECUTOR_INSTANCES:?}" "${SPARK_EXECUTOR_CORES:?}"
: "${SPARK_EXECUTOR_MEMORY:?}" "${SPARK_EXECUTOR_MEMORY_OVERHEAD:?}"
: "${SPARK_DRIVER_MEMORY:?}" "${SPARK_SHUFFLE_PARTITIONS:?}"
: "${SPARK_ADVISORY_PARTITION_SIZE:?}"
export JAVA_HOME HADOOP_HOME HADOOP_CONF_DIR YARN_CONF_DIR SPARK_HOME
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"
if [[ ! -f "${ICEBERG_JAR}" ]]; then
  echo "ERROR: Iceberg runtime JAR not found: ${ICEBERG_JAR}" >&2
  exit 2
fi
SPARK_COMMON=(
  --master yarn
  --deploy-mode client
  --num-executors "${SPARK_EXECUTOR_INSTANCES}"
  --executor-cores "${SPARK_EXECUTOR_CORES}"
  --executor-memory "${SPARK_EXECUTOR_MEMORY}"
  --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD}"
  --conf "spark.pyspark.python=${PYSPARK_PYTHON}"
  --driver-memory "${SPARK_DRIVER_MEMORY}"
  --jars "${ICEBERG_JAR}"
  --conf "spark.sql.session.timeZone=UTC"
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE}"
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
  --conf "spark.sql.adaptive.enabled=true"
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true"
  --conf "spark.sql.adaptive.skewJoin.enabled=true"
  --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=${SPARK_ADVISORY_PARTITION_SIZE}"
  --conf "spark.sql.autoBroadcastJoinThreshold=-1"
  --conf "spark.sql.adaptive.autoBroadcastJoinThreshold=-1"
  --conf "spark.driver.extraJavaOptions=-XX:ActiveProcessorCount=2 -Xss512k"
)
if [[ -n "${SPARK_DRIVER_HOST:-}" ]]; then
  SPARK_COMMON+=(
    --conf "spark.driver.host=${SPARK_DRIVER_HOST}"
    --conf "spark.driver.bindAddress=0.0.0.0"
    --conf "spark.driver.port=7078"
    --conf "spark.blockManager.port=7079"
  )
fi
