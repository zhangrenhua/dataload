package com.hua.spark.dataload.spark.service

import com.hua.spark.dataload.spark.utils.{ConfigUtils, Constants, SparkTypeUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by hua on 2016/4/24.
  */
class DataSaveService extends Serializable {

  /**
    * 将分割后的数组,已文本格式存入hdfs中
    *
    * @param outputPath   输出路径
    * @param outputSplit  输出字段分割符
    * @param columnValues 字段数组RDD
    */
  def saveAsText(outputPath: String, outputSplit: String, columnValues: RDD[Array[String]]): Unit = {

    columnValues.map(columns => {
      // 拼接字符串
      val result: StringBuilder = new StringBuilder()
      for (value: String <- columns) {
        result.append(value).append(outputSplit)
      }

      // 处理最后一个字段
      if (result.nonEmpty) {
        result.substring(0, result.length - outputSplit.length)
      } else {
        result.toString()
      }
    }).saveAsTextFile(outputPath)
  }

  /**
    * 将分割后的数组存入Hive表中
    *
    * @param dataType     数据类型
    * @param tableName    表名
    * @param sc           sparkContext
    * @param columnValues 字段数组RDD
    */
  def saveAsTable(dataType: String, tableName: String, sc: SparkContext, columnValues: RDD[Array[String]]): Unit = {
    // 初始化hiveContext
    val hiveContext: HiveContext = new HiveContext(sc)

    // 创建hive表的字段schema
    val dtypes: Array[(String, String)] = hiveContext.table(tableName).dtypes
    val columnsDataType: Array[StructField] = SparkTypeUtils.generateColumnSchema(dtypes)
    val columnsTypeCode: Array[Int] = SparkTypeUtils.getDataTypeCode(columnsDataType)
    val schema: StructType = DataTypes.createStructType(columnsDataType)

    // 字段类型转换
    val rows: RDD[Row] = columnValues.map(columns => {
      var index: Int = 0
      // 解析字段值
      val columnList: ArrayBuffer[Any] = new ArrayBuffer[Any]()
      for (value <- columns) {
        // 根据字段类型，将字符串解析成与字段类型匹配对象的值
        columnList += SparkTypeUtils.parseValue(columnsTypeCode(index), value, ConfigUtils.config.getConfig(Constants.COLUMNS_FORMAT), dataType, index)
        index += 1
      }
      // 生成hive row对象
      new GenericRow(columnList.toArray[Any])
    })
    // 存入hive
    hiveContext.createDataFrame(rows, schema).write.mode(SaveMode.Append).saveAsTable(tableName)
  }


}
