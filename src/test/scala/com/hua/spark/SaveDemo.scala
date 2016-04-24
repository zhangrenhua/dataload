package com.hua.spark

import java.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, RowFactory, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}
import java.math.BigDecimal
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.catalyst.expressions.GenericRow

/**
  * Created by hua on 2016/4/17.
  */
object SaveDemo {

  def main(args: Array[String]) {
    /**
      * spark 容器初始化
      */
    val sparkConf: SparkConf = new SparkConf().setAppName("demo").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val hiveContext: HiveContext = new HiveContext(sc)

    val columns: util.ArrayList[StructField] = new util.ArrayList[StructField]()
    columns.add(DataTypes.createStructField("a", DataTypes.StringType, true))
    columns.add(DataTypes.createStructField("b", DataTypes.IntegerType, true))
    columns.add(DataTypes.createStructField("c", DataTypes.FloatType, true))
    columns.add(DataTypes.createStructField("d", DataTypes.LongType, true))
    columns.add(DataTypes.createStructField("e", DataTypes.createDecimalType(10, 2), true))
    columns.add(DataTypes.createStructField("f", DataTypes.BooleanType, true))
    columns.add(DataTypes.createStructField("g", DataTypes.DateType, true))
    columns.add(DataTypes.createStructField("h", DataTypes.TimestampType, true))
    val schema: StructType = DataTypes.createStructType(columns)

    val rows: RDD[Row] = sc.textFile("/tmp/input/demo").map {
      case line => {
        val split: Array[String] = line.split("\\|")
        val a = "a"
        val b = 1
        val c = 1.1f
        val d = 111l
        val e = new BigDecimal("10.2")
        val f = true
        val g = new Date(System.currentTimeMillis())
        val h = new Timestamp(System.currentTimeMillis())

        new GenericRow(Array[Any](a,b,c,d,e,f,g,h))
      }
    }

    hiveContext.createDataFrame(rows, schema).write.mode(SaveMode.Append).saveAsTable("default.demo")


  }

}
