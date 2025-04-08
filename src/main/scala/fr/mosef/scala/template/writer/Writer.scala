package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame

trait Writer {
  def write(df: DataFrame, path: String, mode: String = "overwrite"): Unit
}