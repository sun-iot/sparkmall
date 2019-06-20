package com.sun.bigdata.sparkmall.common.util

/**
  * title: StrUtils 
  * projectName sparkmall 
  * description: 
  * author Sun-Smile 
  * create 2019-06-19 13:09 
  */
object StrUtils {
  def isNotEmpty(str :String): Boolean ={
    str != null && !"".equals(str)
  }
}
