package fr.mosef.scala.template.job

import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.DataFrame

trait Job {
  val reader: Reader
  val processor: Processor
  val inputDF: DataFrame
  val groupedDF: DataFrame
  val summedDF: DataFrame
  val stddevDF: DataFrame
}
