package fr.mosef.scala.template

import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

object Main extends App {

  val cliArgs = args

  val MASTER_URL  = if (cliArgs.length > 0) cliArgs(0) else "local[1]"
  val SRC_PATH    = if (cliArgs.length > 1) cliArgs(1) else {
    println("❌ Chemin source requis."); sys.exit(1)
  }
  val DST_PATH    = if (cliArgs.length > 2) cliArgs(2) else "./output"
  val GROUP_VAR   = if (cliArgs.length > 3) cliArgs(3) else ""
  val OP_VAR      = if (cliArgs.length > 4) cliArgs(4) else ""

  // Configuration Spark
  val conf = new SparkConf()
    .set("spark.driver.memory", "64M")
    .set("spark.testing.memory", "471859200")

  implicit val sparkSession: SparkSession = SparkSession.builder()
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.sparkContext.hadoopConfiguration.setClass(
    "fs.file.impl",
    classOf[BareLocalFileSystem],
    classOf[FileSystem]
  )

  // Pipeline
  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl(GROUP_VAR, OP_VAR)
  val writer: Writer = new Writer()

  val inputDF: DataFrame = reader.read(SRC_PATH)

  println("📥 Données d'entrée :")
  inputDF.show(5)

  // Traitements
  //val groupbyDF = processor.groupby(inputDF)
 //val sumDF     = processor.sum(inputDF)
 // val medianeDF = processor.process(inputDF.copy()) // process() = médiane dans ta logique actuelle

  println("📊 Résultat GroupBy :")
  //groupbyDF.show(5)
  println("📊 Résultat Sum :")
  //sumDF.show(5)
  println("📊 Résultat Médiane :")
  //medianeDF.show(5)

  // Sauvegarde
  //writer.write(groupbyDF, s"${DST_PATH}_groupby")
  //writer.write(sumDF,     s"${DST_PATH}_sum")
  //writer.write(medianeDF, s"${DST_PATH}_mediane")

  println(s"Export terminé dans : ${DST_PATH}_*")
}
