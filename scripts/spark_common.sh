#!/usr/bin/env bash
set -euo pipefail
source "${RUNTIME_ENV:-$(dirname "$0")/../configs/runtime.env}"
export JAVA_HOME HADOOP_HOME SPARK_HOME
ICEBERG_PACKAGE="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0"
SPARK_COMMON=(
  --master yarn
  --deploy-mode client
  --num-executors "${SPARK_EXECUTOR_INSTANCES}"
  --executor-cores "${SPARK_EXECUTOR_CORES}"
  --executor-memory "${SPARK_EXECUTOR_MEMORY}"
  --driver-memory "${SPARK_DRIVER_MEMORY}"
  --packages "${ICEBERG_PACKAGE}"
  --conf "spark.sql.session.timeZone=UTC"
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=hive"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.uri=${HIVE_METASTORE_URI}"
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE}"
  --conf "spark.sql.sources.partitionOverwriteMode=dynamic"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE_PARTITIONS}"
  --conf "spark.sql.adaptive.enabled=true"
  --conf "spark.sql.adaptive.coalescePartitions.enabled=true"
  --conf "spark.sql.adaptive.skewJoin.enabled=true"
  --conf "spark.sql.adaptive.advisoryPartitionSizeInBytes=${SPARK_ADVISORY_PARTITION_SIZE}"
)
