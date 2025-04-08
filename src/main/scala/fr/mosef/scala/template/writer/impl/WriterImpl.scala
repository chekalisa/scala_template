package fr.mosef.scala.template.writer.impl

import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.io.FileInputStream
import java.util.Properties

class CSVOutputWriter(configPath: String) extends Writer {

  private val settings = new Properties()
  settings.load(new FileInputStream(configPath))

  override def write(df: DataFrame, mode: String = "overwrite", destination: String): Unit = {
    val includeHeader = settings.getProperty("write_header", "true")

    df.write
      .option("header", includeHeader)
      .mode(mode)
      .csv(destination)
  }
}

class ParquetOutputWriter extends Writer {

  override def write(df: DataFrame, mode: String = "overwrite", destination: String): Unit = {
    df.write
      .mode(mode)
      .parquet(destination)
  }
}

class HiveTableWriter(spark: SparkSession) {

  def writeToTable(df: DataFrame, table: String, mode: String = "overwrite"): Unit = {
    df.write
      .mode(mode)
      .saveAsTable(table)

    println(s"✅ Data successfully written to Hive table: $table")
    println(s"📊 Showing contents of table '$table':")
    spark.table(table).show()
  }
}
