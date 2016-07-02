package com.hua.spark.dataload.spark.service

import com.hua.spark.dataload.common.{BigDataLoadException, CommonUtils}
import com.hua.spark.dataload.spark.utils.Constants
import org.apache.hadoop.io.LongWritable
import org.apache.spark.rdd.RDD

import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator

/**
  * Created by hua on 2016/4/24.
  */
class LineSplitService extends Serializable {


  /**
    * 根据分割类型，将数据进行分割成，字符串数组
    *
    * @param datas          数据RDD
    * @param fieldSplitType 分割类型,fixed:定长,其他:split
    * @param fixedLength    字段长度,如：1,4,10,8
    * @param splitChar      split分隔符
    * @param regex          正则表达式，分割
    * @param columnsLength  分割后的长度，用于校验，大于0才校验
    * @return
    */
  def splitText(datas: RDD[(LongWritable, String)], fieldSplitType: String, fixedLength: String, splitChar: String, regex: Regex, columnsLength: Int): RDD[Array[String]] = {
    var columnValues: RDD[Array[String]] = null
    if (fieldSplitType.equalsIgnoreCase(Constants.FIELD_SPLIT_FIXED)) {
      val fixedLengths: Array[Int] = fixedLength.split(",").map(a => a.toInt)
      columnValues = datas.map {
        case (key, value) => splitFixed(value, fixedLengths, columnsLength)
      }
    } else if (fieldSplitType.equalsIgnoreCase(Constants.COLUMNS_SPLIT_REGEX)) {
      columnValues = datas.map {
        case (key, value) => splitRegex(value, regex, columnsLength)
      }
    } else if (fieldSplitType.equalsIgnoreCase(Constants.FIELD_SPLIT_SPLITSTR)) {
      columnValues = datas.map {
        case (key, value) => splitStr(value, splitChar, columnsLength)
      }
    } else {
      throw new BigDataLoadException("Field split type cannot be empty.")
    }

    columnValues
  }

  /**
    *
    * 利用split分割字符串
    *
    * @param value         字段值
    * @param splitStr      分割符
    * @param cloumnsLength 分割后的长度，用于校验，大于0才校验
    * @return 字段字符串数组
    */
  def splitStr(value: String, splitStr: String, cloumnsLength: Int): Array[String] = {
    // -1:split结果无限定，最大分割
    val columns: Array[String] = value.split(splitStr, -1)
    if (cloumnsLength > 0 && columns.length != cloumnsLength) {
      throw new BigDataLoadException(CommonUtils.append("The columns length is not consistent,lineText:", value, ";splitStr:", splitStr, ";splitLength:" + columns.length))
    }
    columns
  }

  /**
    * 定长分割字段
    *
    * @param value         字段值
    * @param fixedLengths  字段长度数组,如：1,4,10,8
    * @param cloumnsLength 分割后的长度，用于校验，大于0才校验
    * @return 字段字符串数组
    */
  def splitFixed(value: String, fixedLengths: Array[Int], cloumnsLength: Int): Array[String] = {

    val columnsValue: Array[String] = new Array[String](fixedLengths.length)
    var index = 0
    var valueIndex = 0
    val lineBytes = value.getBytes("UTF-8")
    fixedLengths.foreach(valueLength => {

      val columnValue: Array[Byte] = new Array[Byte](valueLength)
      // 定长截取
      System.arraycopy(lineBytes, index, columnValue, 0, valueLength)
      columnsValue(valueIndex) = new String(columnValue, "UTF-8")

      valueIndex += 1
      index += valueLength
    })

    // 校验长度
    if (cloumnsLength > 0 && columnsValue.length != cloumnsLength) {
      throw new BigDataLoadException(CommonUtils.append("The columns length is not consistent,lineText:", value, ";fixedLength:", fixedLengths.toList, ";splitLength:" + columnsValue.length))
    }
    columnsValue
  }

  /**
    *
    * 根据正则表达式截取字段
    *
    * @param value         字段值
    * @param regex         正则表达式
    * @param cloumnsLength 分割后的长度，用于校验，大于0才校验
    * @return
    */
  def splitRegex(value: String, regex: Regex, cloumnsLength: Int): Array[String] = {

    val in: MatchIterator = regex.findAllIn(value)
    val columnsValue: Array[String] = new Array[String](in.groupCount)
    for (i <- 1 to in.groupCount) {
      if (in.hasNext) {
        columnsValue(i - 1) = in.group(i)
      }
    }

    // 校验长度
    if (cloumnsLength > 0 && columnsValue.length != cloumnsLength) {
      throw new BigDataLoadException(CommonUtils.append("The columns length is not consistent,lineText:", value, ";Regex:", regex, ";splitLength:" + columnsValue.length))
    }
    columnsValue
  }

}
