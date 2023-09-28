package org.novakorp.com

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, hash, md5}
import org.apache.spark.sql.types.{StringType, StructType}

object functions extends SparkSessionWrapper {

  def readAvro(landingPath: String): DataFrame = {
    spark.read.format("avro").load(landingPath)
  }

  def readCsv(landingPath: String, outputTable: String): DataFrame = {
    val newDf = spark.read.format("csv")
      .options(Map("header"->"true","delimiter"->"|","nullValue"-> "null"))
      .load(landingPath)

    newDf
  }

  def readFromLanding(landingPath: String, outputTable: String, format: String): DataFrame = {
    // ACA SE DEBERIA CAMBIAR EL FILE PATH O AGREGAR LA FECHA PROCESO EN LANDING.
    val newDf: DataFrame = format match {
      case "csv" => readCsv(landingPath, outputTable)
      case "avro" => readAvro(landingPath)
      case _ => throw new Exception(s"Specified extension ($format) for landing file not found.")}
    newDf
  }

  def readFromTable(query:String): DataFrame = {
    val newDf: DataFrame = spark.sql(query)
    // ACA SE PODRIA AGREGAR UN IF COUNT(TABLA) = 0 => TIRAR ERROR PORQUE LA TABLA ESTA VACIA
    newDf
  }

  def insertDataFrame(df: DataFrame, outputTable: String): Unit = {
    df.write
      .mode("append")
      .option("hive.exec.dynamic.partition", "true")
      .option("hive.exec.dynamic.partition.mode", "nonstrict")
      .insertInto(outputTable)
  }


}
