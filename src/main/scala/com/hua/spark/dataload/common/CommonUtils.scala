package com.hua.spark.dataload.common

/**
  * Created by hua on 2016/4/17.
  */
object CommonUtils {

  /**
    * 多个对象拼接成字符串
    *
    * @param objs 对象数组
    * @return
    */
  def append(objs: Any*): String = {
    val sb: StringBuilder = new StringBuilder
    for (obj <- objs) {
      sb.append(obj)
    }
    sb.toString()
  }

  def isNotBlank(value: String): Boolean = {
    value != null && value.trim.length > 0
  }

}
