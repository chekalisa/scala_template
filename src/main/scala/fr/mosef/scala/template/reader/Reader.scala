package fr.mosef.scala.template.reader

import org.apache.spark.sql.DataFrame

trait Reader {
  def read(path: String): DataFrame
}