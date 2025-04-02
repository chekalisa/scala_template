package fr.mosef.scala.template.reader.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(spark: SparkSession) extends Reader {

  def read(path: String): DataFrame = {
    val format = detectFormat(path.toLowerCase)
    val reader = spark.read.format(format)

    val configuredReader = format match {
      case "csv" =>
        reader.option("header", "true").option("inferSchema", "true")
      case "json" =>
        reader.option("multiline", "true")
      case _ =>
        reader
    }

    configuredReader.load(path)
  }

  private def detectFormat(path: String): String = {
    if (path.endsWith(".csv")) "csv"
    else if (path.endsWith(".parquet")) "parquet"
    else if (path.endsWith(".json")) "json"
    else if (path.endsWith(".avro")) "avro"
    else if (path.endsWith(".orc")) "orc"
    else {
      throw new IllegalArgumentException(s"Format de fichier non reconnu : $path")
    }
  }
}
