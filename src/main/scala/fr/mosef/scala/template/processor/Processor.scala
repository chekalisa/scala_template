package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {

  def groupby(inputDF: DataFrame): DataFrame
  def computeSum(inputDF: DataFrame): DataFrame
  def computeStdDev(inputDF: DataFrame): DataFrame

}