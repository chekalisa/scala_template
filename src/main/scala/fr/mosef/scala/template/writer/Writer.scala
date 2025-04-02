package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame
class Writer {

  def write(df: DataFrame, path: String, mode: String = "overwrite"): Unit = {
    df.write
      .option("header", "true")
      .mode(mode)
      .csv(path)
  }
}