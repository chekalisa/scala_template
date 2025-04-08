package fr.mosef.scala.template.reader.impl

import fr.mosef.scala.template.reader.Reader
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.io.FileInputStream
import java.util.Properties

class CSVReader(spark: SparkSession, configPath: String) extends Reader {

  private val config = new Properties()
  config.load(new FileInputStream(configPath))

  println("=" * 100)
  println("📥 Loading CSV Reader Configuration")
  println(config)
  println("=" * 100)

  override def read(filePath: String): DataFrame = {
    val delimiter = config.getProperty("read_separator")
    val hasHeader = config.getProperty("read_header").toBoolean
    val inferSchema = config.getProperty("schema")
    val format = config.getProperty("read_format_csv")

    val rawData = spark.read
      .option("sep", delimiter)
      .option("header", hasHeader.toString)
      .option("inferSchema", inferSchema)
      .option("multiLine", true)
      .option("quote", "\"")
      .option("escape", "\"")
      .format(format)
      .load(filePath)

    if (!hasHeader) {
      val first = rawData.first()
      val colNames = first.toSeq.map(_.toString)
      rawData.filter(row => row != first).toDF(colNames: _*)
    } else {
      rawData
    }
  }
}

class ParquetReader(spark: SparkSession, configPath: String) extends Reader {

  private val config = new Properties()
  config.load(new FileInputStream(configPath))

  println("=" * 100)
  println("📥 Loading Parquet Reader Configuration")
  println(config)
  println("=" * 100)

  override def read(filePath: String): DataFrame = {
    val format = config.getProperty("read_format_parquet")
    spark.read.format(format).load(filePath)
  }
}

class HiveReader(spark: SparkSession, configPath: String, formatType: String) extends Reader {

  private val config = new Properties()
  config.load(new FileInputStream(configPath))

  println("=" * 100)
  println("📥 Loading Hive Reader Configuration")
  println(config)
  println("=" * 100)

  override def read(filePath: String): DataFrame = {
    val table = config.getProperty("table_name")
    val schema = generateSchema(filePath, formatType)

    createTableIfNeeded(table, schema, formatType)
    insertData(table, filePath, formatType, schema)

    spark.table(table)
  }

  private def generateSchema(filePath: String, formatType: String): StructType = {
    val baseSchema = formatType match {
      case "csv" =>
        spark.read.option("header", "true").option("inferSchema", "true").csv(filePath).schema
      case "parquet" =>
        spark.read.format("parquet").load(filePath).schema
      case other =>
        throw new IllegalArgumentException(s"Unsupported format: $other")
    }

    StructType(baseSchema.map { field =>
      if (Set("ndeg_de_version", "numero_de_contact").contains(field.name))
        field.copy(dataType = LongType)
      else
        field
    })
  }

  private def createTableIfNeeded(tableName: String, schema: StructType, formatType: String): Unit = {
    val storage = formatType match {
      case "csv" => "TEXTFILE"
      case "parquet" => "PARQUET"
      case other => throw new IllegalArgumentException(s"Unsupported storage type: $other")
    }

    val fieldsStr = schema.fields.map(f => s"${f.name} ${f.dataType.simpleString}").mkString(", ")

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS $tableName
         |($fieldsStr)
         |STORED AS $storage
         |""".stripMargin)
  }

  private def insertData(tableName: String, filePath: String, formatType: String, schema: StructType): Unit = {
    val data = formatType match {
      case "csv" =>
        spark.read.option("header", "true").schema(schema).csv(filePath)
      case "parquet" =>
        spark.read.format("parquet").load(filePath)
      case other =>
        throw new IllegalArgumentException(s"Unsupported format: $other")
    }

    data.write.mode(SaveMode.Overwrite).insertInto(tableName)
  }
}
