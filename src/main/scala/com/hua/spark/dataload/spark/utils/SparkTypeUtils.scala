package com.hua.spark.dataload.spark.utils

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import com.typesafe.config.Config
import org.apache.spark.sql.types.{DataType, DataTypes, DecimalType, StructField}

import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * Created by hua on 2016/4/11.
  *
  * DataType代码转换
  *
  */
object SparkTypeUtils {

  /**
    * STRING
    */
  val TYPE_STRING = 0
  val TYPE_INT = 1
  val TYPE_FLOAT = 2
  val TYPE_LONG = 3
  val TYPE_SHORT = 4
  val TYPE_BOOLEAN = 5
  val TYPE_DATE = 6
  val TYPE_TIMESTAMP = 7
  val TYPE_NULL = 8
  val TYPE_DECIMAL = 9
  val TYPE_BYTE = 10

  /**
    *
    * 将Spark类型转换成Int
    *
    * @param dType spark sql数据类型
    * @return
    */
  def getDataTypeCode(dType: DataType): Int = {
    var result = -1
    if (dType == DataTypes.StringType) {
      result = TYPE_STRING
    } else if (dType == DataTypes.IntegerType) {
      result = TYPE_INT
    } else if (dType == DataTypes.FloatType) {
      result = TYPE_FLOAT
    } else if (dType == DataTypes.LongType) {
      result = TYPE_LONG
    } else if (dType.isInstanceOf[DecimalType]) {
      result = TYPE_DECIMAL
    } else if (dType == DataTypes.ShortType) {
      result = TYPE_SHORT
    } else if (dType == DataTypes.BooleanType) {
      result = TYPE_BOOLEAN
    } else if (dType == DataTypes.DateType) {
      result = TYPE_DATE
    } else if (dType == DataTypes.TimestampType) {
      result = TYPE_TIMESTAMP
    } else if (dType == DataTypes.ByteType) {
      result = TYPE_BYTE
    } else if (dType == DataTypes.NullType) {
      result = TYPE_NULL
    } else {
      throw new RuntimeException(dType + " dtype Not supported.")
    }

    result
  }

  def getDataTypeCode(columnsDataType: Array[StructField]): Array[Int] = {
    val columnsCode: ArrayBuffer[Int] = new ArrayBuffer[Int]()

    for (structField <- columnsDataType) {
      columnsCode.append(getDataTypeCode(structField.dataType))
    }
    columnsCode.toArray
  }

  /**
    *
    * 将rdd.dtype返回的字符串数组，转换成Spark sql类型
    *
    * @param dttypes DataType.toString
    * @return
    */
  def generateColumnSchema(dttypes: Array[(String, String)]): Array[StructField] = {

    // Generate the schema based on the string of schema
    val columns: ArrayBuffer[StructField] = new ArrayBuffer[StructField]()
    lazy val regex = new Regex("""\d+,\d+""")

    for (value: (String, String) <- dttypes) {

      val dType = value._2.trim.toLowerCase
      var dataType: DataType = null
      if (dType.startsWith("string")) {
        dataType = DataTypes.StringType
      } else if (dType.startsWith("int")) {
        dataType = DataTypes.IntegerType
      } else if (dType.startsWith("float")) {
        dataType = DataTypes.FloatType
      } else if (dType.startsWith("long")) {
        dataType = DataTypes.LongType
      } else if (dType.startsWith("short")) {
        dataType = DataTypes.ShortType
      } else if (dType.startsWith("boolean")) {
        dataType = DataTypes.BooleanType
      } else if (dType.startsWith("date")) {
        dataType = DataTypes.DateType
      } else if (dType.startsWith("timestamp")) {
        dataType = DataTypes.TimestampType
      } else if (dType.startsWith("decimal")) {
        // 解析字段长度，DecimalType(10,0)
        val array: Array[String] = regex.findFirstIn(dType).get.split(",")
        dataType = DataTypes.createDecimalType(array(0).trim.toInt, array(1).trim.toInt)
      } else if (dType.startsWith("binary")) {
        dataType = DataTypes.BinaryType
      } else if (dType.startsWith("null")) {
        dataType = DataTypes.NullType
      } else {
        throw new RuntimeException(dType + " dtype Not supported.")
      }

      columns.append(DataTypes.createStructField(value._1, dataType, true))
    }
    columns.toArray
  }


  def parseValue(typeCode: Int, value: String, config: Config, dataType: String, columnIndex: Int): Any = {

    // "yyyy-MM-dd HH:mm:ss.S"
    lazy val pattern: String = ConfigUtils.getString(config, "date." + columnIndex)
    lazy val format = new SimpleDateFormat(pattern)
    val resultValue = typeCode match {
      case TYPE_STRING => value;
      case TYPE_INT => value.toInt;
      case TYPE_DECIMAL => new BigDecimal(value);
      case TYPE_DATE => new Date(format.parse(value).getTime);
      case TYPE_TIMESTAMP => new Timestamp(format.parse(value).getTime);
      case TYPE_FLOAT => value.toFloat;
      case TYPE_LONG => value.toLong;
      case TYPE_SHORT => value.toShort;
      case TYPE_BOOLEAN => value.equalsIgnoreCase("true");
      case TYPE_BYTE => value.getBytes;
      case TYPE_NULL => null;
    }

    resultValue
  }


}
