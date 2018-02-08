package com.vega.scorecard.model.config

import com.vega.scorecard.model.utils.JsonUtils


case class ScorecardConfig (name: String,
                            input : InputConfig,
                            model : ModelConfig,
                            validation: ValidationConfig,
                            output : OutputConfig,
                            performance: PerformanceConfig)



case class InputConfig(label_path:String, feature_path:String, split: InputSplitConfig)

case class InputSplitConfig(training: Double, test: Double)


case class ModelConfig(bin : ModelBinConfig,
                       score : ModelScoreScalingConfig,
                       cleanUpUseless : Boolean)

case class ModelBinConfig(use_auto:Boolean, autobin: Option[AutoBinConfig], manual : Option[ModelManualBinConfig])
case class AutoBinConfig(autobin:Int = 20, max_features:Int = 100)
case class ModelManualBinConfig(numeric: Seq[ModelBinNumericConfig],categorical : Seq[ModelBinCategoryConfig], label:String)
case class ModelBinNumericConfig(name: String, quantiles: Seq[Double])
case class ModelBinCategoryConfig(name : String)
case class ModelScoreScalingConfig(target_point: Double, target_odds: Double, pdo: Double, NA: Seq[ModelNAConfig])
case class ModelNAConfig(name: String, value: String, overrideNA: Boolean)


case class ValidationConfig(enable : Boolean, config : ValidationMetaConfig, chart : ValidationChartConfig)
case class ValidationMetaConfig(total_parts : Int, score_postfix : String, score_column_name : String)

case class ValidationChartConfig(chart_type : Seq[String])



case class OutputConfig(model_out_path: String, score : OutputModelScoreConfig)
case class OutputModelScoreConfig(enable : Boolean, score_path: String, histogram : OutputHistogramConfig,
                                  score_column_name : String="score", score_postfix : String="-score")

case class OutputHistogramConfig(enable : Boolean, num_bin : Int=100)

/**
  *
  * @param repartition < 0 using default
  * @param cache use cache or not
  */
case class PerformanceConfig(repartition:Int, cache:Boolean)


object ScorecardConfig {
  def loadJson(jsonText : String) : ScorecardConfig = {
    JsonUtils.parseJson[ScorecardConfig](jsonText, classOf[ScorecardConfig])
  }

  def main(args: Array[String]): Unit = {
    val performanceConfig = PerformanceConfig(10, true)

    val outputConfig = OutputConfig("model_out_path", OutputModelScoreConfig(true, "score_path", OutputHistogramConfig(true, 10)))

    val inputConfig = InputConfig("label_path", "features_path", InputSplitConfig(0.7, 0.3))
    val binNumericConfig = Seq(ModelBinNumericConfig("column1", Seq(1,2,3,4.5)),
      ModelBinNumericConfig("column1", Seq(1,2,3,4.5)),
      ModelBinNumericConfig("column1", Seq(1,2,3,4.5))
    )
    val binCategoryConfig = Seq(
      ModelBinCategoryConfig("col2"),
      ModelBinCategoryConfig("col3"),
      ModelBinCategoryConfig("col4")
    )
    val modelManualBinConfig = ModelManualBinConfig(binNumericConfig, binCategoryConfig, "isdn")

    val modelBinConfig = ModelBinConfig(true, Some(AutoBinConfig(autobin = 20, max_features = 100)), Some(modelManualBinConfig))


    val modelNAConfigs = Seq(
      ModelNAConfig("column1", "null_replacement", true),
      ModelNAConfig("column1", "null_replacement", false)
    )
    val modelScoreScalingConfig = ModelScoreScalingConfig(200.0, 200.0, 20, modelNAConfigs)

    val modelConfig = ModelConfig(modelBinConfig, modelScoreScalingConfig, cleanUpUseless = true)

    val validationConfig = ValidationConfig(enable = true,
      config = ValidationMetaConfig(10, "score", "score"),
      chart = ValidationChartConfig(Seq("1", "2", "3"))
    )


    val scorecardConfig = ScorecardConfig (name = "application",
      input = inputConfig,
      model = modelConfig,
      validation = validationConfig,
      output = outputConfig,
      performance = performanceConfig)

    val json = JsonUtils.toJson(scorecardConfig)
    println(json)

  }
}