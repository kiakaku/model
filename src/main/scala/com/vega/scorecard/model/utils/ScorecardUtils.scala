package com.vega.scorecard.model.utils

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import vn.com.vega.ml.feature.iv.InformationValue

object ScorecardUtils {

  def get_spark(name:String):SparkSession = {
    SparkSession
      .builder()
      .appName(name)
      .getOrCreate()
  }

  def getIV(df:DataFrame, label_cols:String, spark: SparkSession):DataFrame = {
    val (numericPredictors:Set[String], nominalPredictors:Set[String]) = get_predictor(df)
    val (informationValue, _) = InformationValue.createInformationValue(df, numericPredictors, nominalPredictors, label_cols)
    val informationValueModel = informationValue.train()

    val schema = StructType(Seq(StructField("name", StringType), StructField("iv", DoubleType)))
    val all_features = informationValueModel.numericFeatures ++ informationValueModel.nominalFeatures

    val rdd = spark.sparkContext.parallelize[Row](all_features.map(x =>{
      Row(x._1, x._2.iv)
    }).toList)
    spark.sqlContext.createDataFrame(rdd, schema)
  }

  def get_predictor(df:DataFrame):(Set[String], Set[String]) = {
    var numericPredictors: Set[String] = Set()
    var nominalPredictors: Set[String] = Set()
    val ignores = Seq("isdn", "label", "data_date_key")
    df.schema.foreach(f =>{
      if(!ignores.contains(f.name)){
        f.dataType match {
          case IntegerType | DoubleType =>
            numericPredictors += f.name
          case _ =>
            nominalPredictors += f.name
        }
      }
    })
    (numericPredictors, nominalPredictors)
  }
}
