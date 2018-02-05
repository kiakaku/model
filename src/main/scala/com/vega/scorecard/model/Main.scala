package com.vega.scorecard.model

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Calendar

import com.vega.scorecard.model.hdfs.{HdfsFileUtils, HdfsReader}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OptionParser
import vn.com.vega.ml.scoring.scorecard.{CreditScorecard, CreditScorecardModel}

object Main {
  case class Command(model_id:String = null, label_with_hash_path:String = null, features_path:String = null,
                     model_out_path:String = null, score_out_path:String = null, train:Double = 0.7, test:Double = 0.3,
                     cleanUseless:Boolean = false, targetPooint:Double = 200, targetOdds:Double = 2.0, pdo:Double = 20,
                     partitions:Int = 10, autobin:Int = 5){
    def notEmpty(str:String):Boolean = str != null && !str.isEmpty
    def is_valid():Boolean = {
      notEmpty(model_id) && notEmpty(label_with_hash_path) && notEmpty(features_path) && notEmpty(model_out_path) && notEmpty(score_out_path)
    }
  }
  def createParser(): OptionParser[Command] = {
    val parser = new scopt.OptionParser[Command]("scorecard") {
      head("scorecard", "1.0")
      opt[String]('i', "id").required().valueName("<MODEL_ID>").action(
        (x, c) => c.copy(model_id = x.trim)
      ).text("id of model (required)")
      opt[String]('l', "label_path").required().valueName("<label_with_hash_path>").action(
          (x, c) => c.copy(label_with_hash_path = x.trim)
      ).text("data label with hash isdn path (must has 'isdn', 'label' column. comma delimiter with header)")
      opt[String]('f', "features_path").required().valueName("<features_path>").action(
        (x, c) => c.copy(features_path = x.trim)
      ).text("features path in hdfs")
      opt[String]('m', "model_out_path").required().valueName("<label_with_hash_path>").action(
        (x, c) => c.copy(model_out_path = x.trim)
      ).text("data label with hash isdn path")
      opt[String]('s', "score_out_path").required().valueName("<score_out_path>").action(
        (x, c) => c.copy(score_out_path = x.trim)
      ).text("score output path in hdfs")
      opt[Double]('t', "train").valueName("<train>").action(
        (x, c) => c.copy(train = x)
      ).text("training fraction default 0.7")
      opt[Double]('e', "test").valueName("<test>").action(
        (x, c) => c.copy(test = x)
      ).text("testing fraction default 0.3")
      opt[Int]('p', "partitions").valueName("<parttions>").action(
        (x, c) => c.copy(partitions = x)
      ).text("number partition for repartition dataframe")
      opt[Int]('a', "autobin").valueName("<autobin>").action(
        (x, c) => c.copy(autobin = x)
      ).text("autobin")
    }
    parser
  }
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("vega scorecard model")
      .getOrCreate()

    val parser = createParser()
    parser.parse(args, Command()) match {
      case Some(cmd) =>
        if(cmd.is_valid()){
          run(cmd, spark)
        }else{
          parser.showUsageAsError()
          throw new IllegalArgumentException("Cannot parse argument")
        }
      case None =>
        parser.showUsageAsError()
        throw new IllegalArgumentException("Cannot parse argument")
    }
  }


  def run(cmd:Command, spark: SparkSession):Unit = {
    import spark.implicits._
    val label_df = HdfsReader.read_parquet(spark, HdfsFileUtils.get_fullpath(cmd.label_with_hash_path))
      .select("isdn", "label")
    val features_df = HdfsReader.read_parquet(spark, HdfsFileUtils.get_fullpath(cmd.features_path))
    var df = features_df.join(label_df, "isdn")
    val (numericPredictors, nominalPredictors) = get_predictor(df)
    df = df.na.fill("", nominalPredictors.toArray)
    df = df.na.fill(0, numericPredictors.toArray)

    var Array(trainingDf, testingDf) = df.randomSplit(weights = Array(cmd.train,cmd.test))

    trainingDf = trainingDf.repartition(cmd.partitions)
    trainingDf.cache
    println("finished cache")

    val creditScorecard: CreditScorecard = CreditScorecard.createCreditScorecard(trainingDf, numericPredictors,
      nominalPredictors, dfValidation = false, ignoreNA = true)
    println("finished create credit scorecard")
    creditScorecard.autoBin(cmd.autobin)
    println("finished autobin")

    val csModel = creditScorecard.train(cleanUpUseless = cmd.cleanUseless)
    println("finished training")

    csModel.formatPoint(cmd.targetPooint, cmd.targetOdds, cmd.pdo)
    val json_model = CreditScorecardModel.serialize(csModel)
    println(json_model)
//
//    val scoreDf:DataFrame = csModel.score(features_df)
//    val model_id = generate_model_id()
//    val curr_date = get_curr_date()
//    val model_df = spark.sparkContext.parallelize(Seq(model_id, curr_date, json_model))
//      .toDF("model_id", "data_date_key", "model")
//    model_df.write.partitionBy("data_date_key")
//      .mode(SaveMode.Overwrite)
//      .parquet(HdfsFileUtils.get_fullpath(cmd.model_out_path))
//    scoreDf
//      .withColumn("data_date_key", lit(curr_date))
//      .withColumn("model_id", lit(model_id))
//      .write.mode(SaveMode.Overwrite)
//      .partitionBy("data_date_key")
//      .parquet(HdfsFileUtils.get_fullpath(cmd.score_out_path))
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

  val format:DateFormat = new SimpleDateFormat("yyyyMMdd")

  def get_curr_date():String = {
    format.format(Calendar.getInstance().getTime)
  }

  def generate_model_id():String = {
    "scorecard_model_" + System.nanoTime()
  }
}
