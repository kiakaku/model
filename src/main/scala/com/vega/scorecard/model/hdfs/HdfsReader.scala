package com.vega.scorecard.model.hdfs

import org.apache.spark.sql.{DataFrame, SparkSession}

object HdfsReader {

  def read_csv(spark:SparkSession, path:String, delimiter:String = ",", header:Boolean = true):DataFrame = {
    spark.read
      .option("delimiter", delimiter)
      .option("header", header)
      .option("inferSchema", "true")
      .csv(path)
  }

  def read_parquet(spark: SparkSession, path:String):DataFrame = {
    spark.read.parquet(path)
  }
}
