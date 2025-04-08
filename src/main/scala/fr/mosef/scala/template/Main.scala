package fr.mosef.scala.template

import fr.mosef.scala.template.job.Job
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.{CSVReader, HiveReader, ParquetReader}
import fr.mosef.scala.template.writer.Writer
import fr.mosef.scala.template.writer.impl.{CSVOutputWriter, ParquetOutputWriter, HiveTableWriter}

import org.apache.spark.sql.SparkSession

object Main extends App with Job {

  // Récupération des arguments CLI avec des valeurs par défaut
  val argsList = args

  val masterUrl: String = if (argsList.length > 0) argsList(0) else "local[1]"

  val inputPath: String = if (argsList.length > 1) argsList(1) else {
    println("⚠️  Aucun chemin d’entrée fourni.")
    sys.exit(1)
  }

  val outputPath: String = if (argsList.length > 2) argsList(2) else "./default/output-writer"
  val groupColumn: String = if (argsList.length > 3) argsList(3) else "group_key"
  val targetColumn: String = if (argsList.length > 4) argsList(4) else "field1"
  val useHive: Boolean = if (argsList.length > 5) argsList(5).toBoolean else false
  val propertiesPath: String = if (argsList.length > 6) argsList(6) else "./src/main/resources/application.properties"

  println("=" * 100)
  println("🔧 Paramètres de l'application")
  println("=" * 100)
  println(s"🔗 Spark master : $masterUrl")
  println(s"📥 Chemin source : $inputPath")
  println(s"📤 Chemin destination : $outputPath")
  println(s"🔑 Colonne de regroupement : $groupColumn")
  println(s"📊 Colonne d'opération : $targetColumn")
  println(s"📁 Fichier de configuration : $propertiesPath")
  println(s"🐝 Utilisation de Hive : $useHive")
  println("=" * 100)

  // Initialisation de Spark
  val spark = SparkSession
    .builder()
    .master(masterUrl)
    .enableHiveSupport()
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // Détermination du type de fichier (csv, parquet, etc.)
  val fileType = inputPath.split("\\.").lastOption.getOrElse("")

  // Sélection du lecteur en fonction du format et du mode Hive
  val reader: Reader = if (useHive) {
    new HiveReader(spark, propertiesPath, fileType)
  } else {
    fileType match {
      case "csv" => new CSVReader(spark, propertiesPath)
      case "parquet" => new ParquetReader(spark, propertiesPath)
      case _ =>
        println(s"❌ Format de fichier non pris en charge : $fileType")
        sys.exit(1)
    }
  }
  // Création du processeur
  val processor: Processor = new ProcessorImpl(groupColumn, targetColumn)

  // Lecture des données
  val inputDF = reader.read(inputPath)

  println("\n🧾 Aperçu des données d'entrée")
  inputDF.show(5)

  // Exécution des traitements
  val groupedDF = processor.groupby(inputDF)
  val summedDF = processor.computeSum(inputDF)
  val stddevDF = processor.computeStdDev(inputDF)

  // Écriture des résultats (fichiers ou Hive selon le mode)
  if (!useHive) {
    val dst_path = outputPath

    val writer: Writer = fileType match {
      case "csv" => new CSVOutputWriter(propertiesPath)
      case "parquet" => new ParquetOutputWriter()
      case _ => sys.exit(1)
    }

    println("\n📈 Résultat - GroupBy")
    groupedDF.show(5)

    println("\n➕ Résultat - Somme")
    summedDF.show(5)

    println("\n📉 Résultat - Écart-type")
    stddevDF.show(5)

    writer.write(groupedDF, "overwrite", dst_path + "_groupby")
    writer.write(summedDF, "overwrite", dst_path + "_sum")
    writer.write(stddevDF, "overwrite", dst_path + "_stddev")

  } else {
    // Cas Hive
    val writer = new HiveTableWriter(spark)
    writer.writeToTable(groupedDF, "table_groupby")
    writer.writeToTable(summedDF, "table_sum")
    writer.writeToTable(stddevDF, "table_stddev")
  }
}
