package com.vega.scorecard.model

import com.vega.scorecard.model.hdfs.HdfsFileUtils
import com.vega.scorecard.model.utils.ScorecardUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import vn.com.vega.ml.scoring.scorecard.CreditScorecardModel

import scala.collection.mutable.ListBuffer

object Scoring {
  def main(args: Array[String]): Unit = {
    /*
    model_id
    mode_path: df store all model
    df_path
    score_out_path:
     */


    val model_id = args(0)
    val model_path = args(1)
    val df_path = args(2)
    val score_out_path = args(3)
    val spark = ScorecardUtils.get_spark("Scoring")
    val modelDf = HdfsFileUtils.read_parquet(spark, model_path)
      .filter("id = " + model_id).collect()
    if(modelDf.length <= 0){
      throw new Exception("Cannot find any model with id: " + model_id)
    }else{
      val csModel = CreditScorecardModel.deserialize(modelDf.head.getAs[String]("model"))


      df_path.split(",").foreach(path => {
        var curr_df = HdfsFileUtils.read_parquet(spark, df_path)
        val select_cols = get_predictor(curr_df.schema, csModel.predictors)
        curr_df = curr_df.select(select_cols.head, select_cols.tail: _*)
      })

//      val score = csModel.score(df, scoresColumnName = "score", scorePostfix = "_score")


    }
  }

  def get_predictor(schema:StructType, predictor:Set[String]):List[String] = {
    val ls:ListBuffer[String] = ListBuffer[String]()
    schema.foreach(field => {
      if(predictor.contains(field.name) || "isdn".equals(field.name)){
        ls += field.name
      }
    })
    ls.toList
  }
}
