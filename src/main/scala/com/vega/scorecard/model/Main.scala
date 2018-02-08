package com.vega.scorecard.model

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar

import com.vega.scorecard.model.config.{AutoBinConfig, ConfigLoader, ScorecardConfig, ScorecardConfigType}
import com.vega.scorecard.model.hdfs.HdfsFileUtils
import com.vega.scorecard.model.utils.ScorecardUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, _}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import vn.com.vega.ml.feature.iv.InformationValue
import vn.com.vega.ml.scoring.scorecard.CreditScorecard

import scala.util.Random

object Main {

  def main(args: Array[String]): Unit = {

    val spark = ScorecardUtils.get_spark("scorecard training model")
    val configType = ScorecardConfigType.withName(args(0))
    val config_path = args(1)
    val config:ScorecardConfig = ConfigLoader.load(spark, config_path, configType)


  }

  def run(spark:SparkSession, config: ScorecardConfig):Unit = {
    val inputConfig = config.input
    val label_df = HdfsFileUtils.read_parquet(spark, HdfsFileUtils.get_fullpath(inputConfig.label_path))
      .select("isdn", "label")
    val features_df = HdfsFileUtils.read_parquet(spark, HdfsFileUtils.get_fullpath(inputConfig.feature_path))
    var df = features_df.join(label_df, "isdn")
    val (numericPredictors, nominalPredictors) = get_predictor(df)
    df = df.na.fill("", nominalPredictors.toArray)
    df = df.na.fill(0, numericPredictors.toArray)

    var Array(trainingDf, testingDf) = df.randomSplit(weights = Array(inputConfig.split.training, inputConfig.split.test))

    trainingDf = trainingDf.repartition(config.performance.repartition)
    if(config.performance.cache){
      trainingDf.cache
    }
    println("finished cache")

    val creditScorecard: CreditScorecard = CreditScorecard.createCreditScorecard(trainingDf, numericPredictors,
      nominalPredictors, dfValidation = false, ignoreNA = true)
    println("finished create credit scorecard")
    val binConfig = config.model.bin
    if(binConfig.use_auto){
      val autoBinConfig = binConfig.autobin.getOrElse(AutoBinConfig())
      creditScorecard.autoBin(autoBinConfig.autobin)
      println("finished autobin")


    }else{
      throw new Exception("Unsupport manual bin")
    }


  }

  def getIV(df:DataFrame, numericPredictors:Set[String], nominalPredictors:Set[String], label_cols:String, spark: SparkSession):DataFrame = {
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

  def generate_df(rows:Int, cols:Int, spark: SparkSession): DataFrame = {
    val data = (1 to rows).map(_ => Seq.fill(cols)(Random.nextInt(10)))
    val cols_name = (1 to cols).map(i => "column_" + i)
    val sche = StructType(cols_name.map(fieldName => StructField(fieldName, IntegerType, true)))
    val rdd: RDD[Row] = spark.sparkContext.parallelize(data.map(x => Row.fromSeq(x)))
    spark.sqlContext.createDataFrame(rdd, sche)
  }

  val format:DateFormat = new SimpleDateFormat("yyyyMMdd")

  def get_curr_date():String = {
    format.format(Calendar.getInstance().getTime)
  }

  def generate_model_id():String = {
    "scorecard_model_" + System.nanoTime()
  }
}
