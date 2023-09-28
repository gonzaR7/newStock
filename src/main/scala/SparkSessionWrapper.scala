package org.novakorp.com

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


trait SparkSessionWrapper extends Serializable {

  lazy val conf: SparkConf = new SparkConf()
    .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .setAppName("Insert-Stock")
    .set("hive.exec.dynamic.partition", "true")
    .set("hive.exec.dynamic.partition.mode", "nonstrict")
    //.setMaster("yarn")
    //.set("spark.submit.deployMode","client")
    .set("spark.kryoserializer.buffer.max", "1024m")
    .set("spark.dynamicAllocation.enabled", "true")
    .set("spark.dynamicAllocation.minExecutors", "1")
    .set("spark.dynamicAllocation.maxExecutors", "12")
    .set("spark.sql.shuffle.partitions", "20")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    .set("spark.sql.shuffle.partitions", "50")
    .set("spark.sql.autoBroadcastJoinThreshold", "-1")
    //.set("spark.sql.warehouse.dir", "abfs://data@envlargddev.dfs.core.windows.net/LAR_DATA")
    .set("spark.sql.hive.metastore.version", "2.1.1") // 3.1.2 (VERSION DE HIVE) 2.1.1 version metastore
    .set("spark.sql.hive.metastore.jars", "/usr/hdp/current/spark3-client/jars/*")
    .set("spark.sql.hive.metastore.jars.ivy", "/usr/hdp/current/spark3-client/jars/ivy-2.4.0.jar")
    .set("spark.shuffle.service.enabled", "true")
    .set("spark.sql.debug.maxToStringFields", "1000")

  lazy val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

}
