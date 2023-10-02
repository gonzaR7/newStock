package org.novakorp.com
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object entry extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val S0Table: String = args(0) //"cur.mg_hogar_existencia_inicial"
    val stockTable: String = args(1) //"cur.mg_hogar_stock"
    val sucursal: String = args(2) // "CRESPO-VETE"
    val fechaInicial: String = args(3) //"2022-07-01"
    val fechaFinal: String = args(4) //"2022-07-05"

    // SE ARMAN LOS DF NECESARIOS
    val query_movimiento = s"SELECT codigo_articulo, fecha, cantidad_movimiento, multiplicador_stock FROM cur.movimientos_detalle_unificado where sucursal = '$sucursal' AND fecha BETWEEN '${fechaInicial}' AND '$fechaFinal'"
    val df_movimientos = spark.sql(query_movimiento)

    val query_ei = s"SELECT codigo_interno,existencia, fecha_existencia FROM $S0Table"
    val df_ei = spark.sql(query_ei).withColumn("existencia", col("existencia").cast("float")).withColumn("fecha_stock", to_date(lit("2023-06-30"), "yyyy-MM-dd")).drop("fecha_existencia")

    val query_stock_actual = s"SELECT * FROM $stockTable"
    val df_stock = spark.sql(query_stock_actual)

    if (spark.table(stockTable).isEmpty) {

      println("Ingestando el Stock Inicial...")

      // Cargar solamente por única vez (Carga stock_inicial)
      val dfToInsert: DataFrame = df_ei.withColumn("fecha_proceso", date_format(col("fecha_stock"), "yyyyMMdd"))
      functions.insertDataFrame(dfToInsert, {stockTable}, "append")

    }

    // Se buscan los artículos que no están en existencia_inicial pero que sí tienen movimientos en la sucursal X
    val df_ei_codes = df_ei.select("codigo_interno")
    val df_mov_codes = df_movimientos.select("codigo_articulo").distinct()
    val df_new_codes = df_mov_codes.except(df_ei_codes)
    val dfToInsertNew = df_new_codes.withColumn("fecha_proceso", lit("20230630")).withColumn("fecha_stock", lit("2023-06-30")).withColumn("existencia",lit(0)).select(col("codigo_articulo").as("codigo"), col("existencia").cast("float"), col("fecha_stock").cast("date"), col("fecha_proceso"))
    functions.insertDataFrame(dfToInsertNew, {stockTable},"append")

    println("Se ingestaron los nuevos códigos de esta sucursal")

    println("")
    println(s"Se va a ingestar desde $fechaInicial hasta $fechaFinal")
    println("")

    //AGRUPO MOVIMIENTOS POR DIA PARA CADA CODIGO
    val df_stock_mov_agrupado = df_movimientos.groupBy("codigo_articulo", "fecha").agg(sum(col("cantidad_movimiento") * col("multiplicador_stock")).as("suma_movimientos"))

    val df_stock_agrupado = df_stock.filter(f"fecha_stock<'$fechaInicial'").groupBy("codigo").agg(max("fecha_stock").alias("max_fecha_stock")).withColumnRenamed("codigo","codigo_art")

    //ME TRAIGO EL STOCK PARA LA FECHA ANTERIOR OBTENIDA
    val df_stock_actual = df_stock.join(df_stock_agrupado,df_stock("codigo")===df_stock_agrupado("codigo_art") && df_stock("fecha_stock")===df_stock_agrupado("max_fecha_stock"),"inner").select(df_stock("codigo"),df_stock("fecha_stock"),df_stock("existencia"))

    //UNO LA EL STOCK DE LA FECHA ANTERIOR CON LOS MOVIMIENTOS A ACTUALIZAR PARA LUEGO SUMARLOS
    val df_union=df_stock_actual.unionAll(df_stock_mov_agrupado)

    val ventanaPorCodigo = Window.partitionBy("codigo").orderBy("fecha_stock")
    val ventanaPorCodigoYOrden = Window.partitionBy("codigo").orderBy("orden")

    //HAGO EL CALCULO DE LA EXISTENCIA DE STOCL CON EL ANTERIOR REGISTRO Y FILTRO LA FILA QUE ME TRAJE DE REFERENCIA
    val df_final = df_union.withColumn("orden", row_number().over(ventanaPorCodigo)).withColumn("cantidad_acumulativa", sum("existencia").over(ventanaPorCodigoYOrden)).filter("orden>1").drop("orden").drop("existencia").withColumnRenamed("cantidad_acumulativa","existencia").select("codigo","existencia","fecha_stock")

    val dfToInsert = df_final.withColumn("fecha_proceso", date_format(col("fecha_stock"),"yyyyMMdd"))

    functions.insertDataFrame(dfToInsert, {stockTable}, "overwrite")

    }
}
