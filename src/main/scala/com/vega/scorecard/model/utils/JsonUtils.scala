package com.vega.scorecard.model.utils

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper


object JsonUtils {
  def toJson(data : AnyRef) : String = {
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
      .enable(SerializationFeature.INDENT_OUTPUT)
    val jsonText = mapper.writeValueAsString(data)
    jsonText
  }
  def parseJson[T](jsonText: String,mClass : Class[T]) : T = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
      .configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false)
    val rs = mapper.readValue(jsonText, mClass)
    rs
  }
}
