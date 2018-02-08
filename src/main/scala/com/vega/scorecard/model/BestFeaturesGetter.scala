package com.vega.scorecard.model

import com.vega.scorecard.model.hdfs.HdfsFileUtils
import com.vega.scorecard.model.utils.ScorecardUtils
import org.apache.spark.sql.{DataFrame, SaveMode}

object BestFeaturesGetter {
  def main(args: Array[String]): Unit = {
    val spark = ScorecardUtils.get_spark("get best features")
    if(args.length < 3) throw new IllegalArgumentException("You must have one input_path at least, one label path and one output path")
    val label_path = args(args.length - 2)
    val output_path = args(args.length - 1)

    val labelDf = HdfsFileUtils.read_parquet(spark, HdfsFileUtils.get_fullpath(label_path)).cache()

    var ivDf:DataFrame = null
    for(i <- 0 until args.length - 2){
      val currDf = HdfsFileUtils.read_parquet(spark, HdfsFileUtils.get_fullpath(args(i)))
      val df_with_label = currDf.join(labelDf, "isdn").cache()
      val currIv = ScorecardUtils.getIV(df_with_label, "label", spark)
      if(ivDf == null){
        ivDf = currIv
      }else{
        ivDf = ivDf.union(currIv)
      }
    }
    ivDf.write.mode(SaveMode.Overwrite).parquet(HdfsFileUtils.get_fullpath(args(args.length - 1)))

  }

}
