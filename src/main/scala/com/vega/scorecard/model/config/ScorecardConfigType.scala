package com.vega.scorecard.model.config

object ScorecardConfigType extends Enumeration{
  val HDFS = Value(name = "HDFS")
  val LOCAL = Value(name = "LOCAL")
  val DATABASE = Value(name = "DATABASE")
}
