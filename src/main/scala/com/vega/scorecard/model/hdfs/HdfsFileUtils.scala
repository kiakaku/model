package com.vega.scorecard.model.hdfs

import java.io.BufferedOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

object HdfsFileUtils {

  val hdfs_master = "hdfs://name-node01:8020/"
  def get_fullpath(path:String ):String = {
    hdfs_master + path
  }

  def read_csv(spark:SparkSession, path:String, delimiter:String = ",", header:Boolean = true):DataFrame = {
    spark.read
      .option("delimiter", delimiter)
      .option("header", header)
      .option("inferSchema", "true")
      .csv(hdfs_master + path)
  }
  def read_parquet(spark: SparkSession, path:String):DataFrame = {
    spark.read.parquet(hdfs_master + path)
  }

  def read_text_file(spark: SparkSession, path:String):String = {
    spark.read.textFile(hdfs_master + path).collect()(0)
  }
}
