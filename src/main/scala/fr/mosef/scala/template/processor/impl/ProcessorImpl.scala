package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, stddev, sum}

class ProcessorImpl(groupvar: String, op_var: String) extends Processor {
  private def toNumeric(df: DataFrame, columnName: String): DataFrame = {
    df.withColumn(columnName, col(columnName).cast("double"))
  }

  def groupby(inputDF: DataFrame): DataFrame = {
    val dfNumeric = toNumeric(inputDF, op_var)
    dfNumeric.groupBy(groupvar).sum(op_var)
  }

  def computeSum(inputDF: DataFrame): DataFrame = {
    val dfNumeric = toNumeric(inputDF, op_var)
    dfNumeric.agg(sum(op_var).alias("sum"))
  }

  def computeStdDev(inputDF: DataFrame): DataFrame = {
    val dfNumeric = toNumeric(inputDF, op_var)
    dfNumeric.agg(stddev(op_var).alias("stddev"))
  }
}
