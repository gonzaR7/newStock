package org.novakorp.com

import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.storage.StorageLevel
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object entry extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val S0Table: String = args(0) //"raw_cr.vete_existencia_inicial"
    val stockTable: String = args(1) //"raw_cr.vete_stock"
    val sucursal: String = args(2) // "CRESPO-VETE"
    val fechaInicial: String = args(3) //"2022-07-01"
    val fechaFinal: String = args(4) //"2022-07-05"

    // SE ARMAN LOS DF NECESARIOS
    val query_movimiento = s"SELECT codigo_articulo, fecha, cantidad_movimiento, multiplicador_stock FROM cur.movimientos_detalle_unificado where sucursal = '${sucursal}' AND fecha BETWEEN '${fechaInicial}' AND '${fechaFinal}'"
    val df_movimientos = spark.sql(query_movimiento).cache()

    val query_ei = s"SELECT codigo_interno,existencia, fecha_existencia FROM ${S0Table}"
    val df_ei = spark.sql(query_ei).withColumn("existencia", col("existencia").cast("float")).withColumn("fecha_stock", to_date(lit("2023-06-30"), "yyyy-MM-dd")).drop("fecha_existencia")

    if (spark.table(stockTable).isEmpty) {

      println("Ingestando el Stock Inicial...")

      // Cargar solamente por única vez (Carga stock_inicial)
      val dfToInsert: DataFrame = df_ei.withColumn("fecha_proceso", date_format(col("fecha_stock"), "yyyyMMdd"))
      functions.insertDataFrame(dfToInsert, {stockTable})

    }

    // Se buscan los artículos que no están en existencia_inicial pero que sí tienen movimientos en la sucursal X
    val df_ei_codes = df_ei.select("codigo_interno")
    val df_mov_codes = df_movimientos.select("codigo_articulo").distinct()
    val df_new_codes = df_mov_codes.except(df_ei_codes)
    val dfToInsertNew = df_new_codes.withColumn("fecha_proceso", lit("20230630")).withColumn("fecha_stock", lit("2023-06-30")).withColumn("existencia",lit(0)).select(col("codigo_articulo").as("codigo"), col("existencia").cast("float"), col("fecha_stock").cast("date"), col("fecha_proceso"))
    functions.insertDataFrame(dfToInsertNew, {stockTable})
    println("Se ingestaron los nuevos códigos de esta sucursal")

    val fecha_inicio = LocalDate.parse(fechaInicial).toEpochDay
    val fecha_final = LocalDate.parse(fechaFinal).toEpochDay

    println("")
    println(s"Se va a ingestar desde ${fechaInicial} hasta ${fechaFinal}")
    println("")


    for (date <- fecha_inicio to fecha_final) {

      val currentDate = LocalDate.ofEpochDay(date)

      println("")
      println(s"--------------- Se está ejecutando el día ${currentDate} ---------------")
      println("")

      val df_mov_filtrado = df_movimientos.filter(col("fecha") === currentDate)
      val df_stock_mov_agrupado = df_mov_filtrado.groupBy("codigo_articulo", "fecha").agg(sum(col("cantidad_movimiento") * col("multiplicador_stock")).as("suma_movimientos"))
      df_stock_mov_agrupado.createOrReplaceTempView("df_stock_mov_agrupado_temp")
      val query_stock = s"SELECT codigo,existencia,fecha_stock FROM ${stockTable} s JOIN df_stock_mov_agrupado_temp m ON s.codigo = m.codigo_articulo"
      val df_stock = spark.sql(query_stock)
      val query_max_fecha = s"SELECT s.codigo, max(s.fecha_stock) as fecha_maxima FROM ${stockTable} s JOIN df_stock_mov_agrupado_temp m ON s.codigo = m.codigo_articulo GROUP BY s.codigo"
      val df_max_fecha = spark.sql(query_max_fecha)
      val df_stock_2 = df_max_fecha.join(df_stock, Seq("codigo"), "inner").where(df_max_fecha("fecha_maxima") === df_stock("fecha_stock"))
      val df_stock_mov = df_stock_2.as("stock").join(df_stock_mov_agrupado.as("mov"), col("codigo") === col("codigo_articulo"), "inner").select(col("codigo"), col("existencia"), col("fecha").as("fecha_stock"), col("suma_movimientos"))
      val df_stock_mov_existencia = df_stock_mov.withColumn("existencia", col("existencia") + col("suma_movimientos")).drop("suma_movimientos")
      val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val dfToInsert = df_stock_mov_existencia.withColumn("fecha_proceso", lit(currentDate.format(formatter)))

      functions.insertDataFrame(dfToInsert, {stockTable})

    }
  }
}
