package com.vega.scorecard.model.config

import com.vega.scorecard.model.hdfs.HdfsFileUtils
import com.vega.scorecard.model.utils.JsonUtils
import org.apache.spark.sql.SparkSession


object ConfigLoader {


  def load(spark:SparkSession, path:String, configType: ScorecardConfigType.Value):ScorecardConfig = {
    configType match {
      case  ScorecardConfigType.HDFS => {
        val json =  HdfsFileUtils.read_text_file(spark, path)
        return ScorecardConfig.loadJson(json)
      }
      case _ =>{
        throw new Exception("Unsupport config type " + configType.toString)
      }
    }
    null
  }
}

