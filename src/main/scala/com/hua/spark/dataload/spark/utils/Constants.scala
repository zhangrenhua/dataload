package com.hua.spark.dataload.spark.utils

/**
  * Created by hua on 2016/4/24.
  */
object Constants {

  /**
    * 数据目录
    */
  val DATA_INPUT_PATH = "data.input.path"

  /**
    * 字符解码
    */
  val FILE_TEXT_DECODE = "file.text.decode"

  /**
    * 行的正则匹配表达式
    */
  val DATA_INPUT_LINE_REGEX = "data.input.line.regex.str"

  /**
    * 正则不匹配则异常退出
    */
  val DATA_INPUT_LINE_REGEX_ERR_EXIT = "data.input.line.regex.err.exit"

  /**
    * 字段分割类型，fixed:定长,splitstr:字符分割
    */
  val COLUMNS_SPLIT_TYPE = "columns.split.type"

  /**
    * 字段分隔符
    */
  val COLUMNS_SPLIT_SPLITSTR_STR = "columns.split.splitstr.str"

  /**
    * 字段定长分割，如：10,12,34
    */
  val COLUMNS_SPLIT_FIXED_COLUMNS_LENGTH = "columns.split.fixed.columns.length"

  /**
    * 字段长度，用于分割后校验,-1不校验
    */
  val COLUMNS_FIXED_LENGTH = "columns.fixed.length"

  /**
    * 正则表达式分割
    */
  val COLUMNS_SPLIT_REGEX = "columns.split.regex"

  /**
    * 正则表达式
    */
  val COLUMNS_REGEX_STR = "regex.str"

  /**
    * 格式化
    */
  val COLUMNS_FORMAT = "columns.format"

  /**
    * 数据存储类型,hive：存入hive表，text:数据目录
    */
  val DATA_STORE_TYPE = "data.store.type"

  /**
    * 类型为hive：表名
    */
  val DATA_STORE_HIVE_TABLENAME = "data.store.hive.tablename"

  /**
    * 类型为text：输出目录
    */
  val DATA_STORE_TEXT_OUTPUT_PATH = "data.store.text.output.path"

  /**
    * 类型为text：存储的分割符
    */
  val DATA_STORE_TEXT_COLUMNS_SPLITSTR = "data.store.text.columns.splitstr"


}
