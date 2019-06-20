package com.sun.bigdata.sparkmall.common.util

import java.io.InputStream
import java.util.{Properties, ResourceBundle}

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * title: ConfigUtils 
  * projectName sparkmall 
  * description: 
  * author Sun-Smile 
  * create 2019-06-18 15:57 
  */
object ConfigUtils {
  val condRb =  ResourceBundle.getBundle("condition")
  def getValueConfig(key:String):String={
    val stream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties")
    val properties = new Properties()
    properties.load(stream)
    properties.getProperty(key)
  }

  def getValueCondition(key : String): String ={
    val str: String = condRb.getString("condition.params.json")
    val jSONObject: JSONObject = JSON.parseObject(str)
    jSONObject.getString(key)
  }

  def main(args: Array[String]): Unit = {
    println(getValueCondition("endDate"))
  }
}
