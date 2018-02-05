package com.vega.scorecard.model.hdfs

import java.io.BufferedOutputStream

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object HdfsFileUtils {

  val hdfs_master = "hdfs://scorecard-namenode:8020/"
  def get_fullpath(path:String ):String = {
    hdfs_master + path
  }
}
