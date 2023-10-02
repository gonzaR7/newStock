package org.novakorp.com

import org.apache.spark.sql.DataFrame

object functions extends SparkSessionWrapper {

  def insertDataFrame(df: DataFrame, outputTable: String, mode: String): Unit = {
    df.write
      .mode(f"$mode")
      .option("hive.exec.dynamic.partition", "true")
      .option("hive.exec.dynamic.partition.mode", "nonstrict")
      .insertInto(outputTable)
  }
}