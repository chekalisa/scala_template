package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ProcessorImpl(groupvar: String = "", op_var: String = "", mode: String = "sum")(implicit spark: SparkSession) extends Processor {

  private def toNumeric(df: DataFrame, columnName: String): DataFrame = {
    df.withColumn(columnName, col(columnName).cast("double"))
  }

  override def process(inputDF: DataFrame): DataFrame = {
    val dfNumeric = toNumeric(inputDF, op_var)

    mode match {
      case "sum" =>
        dfNumeric.agg(sum(op_var).alias("sum"))

      case "mediane" =>
        // Approximate median using approxQuantile
        val median = dfNumeric.stat.approxQuantile(op_var, Array(0.5), 0.0).headOption.getOrElse(Double.NaN)
        import spark.implicits._
        Seq((median)).toDF("mediane")

      case "groupby" =>
        dfNumeric.groupBy(groupvar).sum(op_var)

      case _ =>
        throw new IllegalArgumentException(s"Mode '$mode' non reconnu.")
    }
  }
}
