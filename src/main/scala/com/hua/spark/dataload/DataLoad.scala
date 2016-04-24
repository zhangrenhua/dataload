package com.hua.spark.dataload

import java.util
import java.util.Map.Entry

import com.hua.spark.dataload.common.CommonUtils
import com.hua.spark.dataload.hadoop.{InputFileFilter, TextInputFormat}
import com.hua.spark.dataload.spark.service.{DataSaveService, LineFilterService, LineSplitService}
import com.hua.spark.dataload.spark.utils.{ConfigUtils, Constants}
import com.typesafe.config.{Config, ConfigValue}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.matching.Regex

/**
  * Created by hua on 2016/3/29.
  */
object DataLoad {

  private val LOG: Logger = LoggerFactory.getLogger(DataLoad.getClass)

  def main(args: Array[String]) {

    if (args.length < 2) {
      LOG.error(
        """数据类型不能为空,运行示例：dataload.jar [datatype] [configName]
          datatype：数据类型，配置文件中的root路径
          configName：配置文件路径，只支持classes路径，请将配置文件上传到classes路径：spark-submit --file configPath
        """.stripMargin)

      System.exit(-1)
    }

    /**
      * 加载配置文件
      */
    val dataType: String = args(0)
    val configName: String = args(1)
    ConfigUtils.load(configName, dataType)
    // 初始化hadoop参数配置
    val hadoopConf: Configuration = initHadoopConfig(args, dataType, ConfigUtils.config)

    /**
      * Input
      */
    val inputPath: String = ConfigUtils.getString(Constants.DATA_INPUT_PATH)
    val decode: String = ConfigUtils.getString(Constants.FILE_TEXT_DECODE, "UTF-8")
    val lineRegex: String = ConfigUtils.getString(Constants.DATA_INPUT_LINE_REGEX, "")
    lazy val errExit: Boolean = ConfigUtils.getBoolean(Constants.DATA_INPUT_LINE_REGEX_ERR_EXIT)

    /**
      * 字段相关
      */
    val fieldSplitType: String = ConfigUtils.getString(Constants.COLUMNS_SPLIT_TYPE)
    val splitChar: String = ConfigUtils.getString(Constants.COLUMNS_SPLIT_SPLITSTR_STR, "")
    val regex: Regex = new Regex(ConfigUtils.getString(Constants.COLUMNS_SPLIT_REGEX + "." + Constants.COLUMNS_REGEX_STR), "")
    val fixedLength: String = ConfigUtils.getString(Constants.COLUMNS_SPLIT_FIXED_COLUMNS_LENGTH, "")
    val columnsLength: Int = ConfigUtils.getInt(Constants.COLUMNS_FIXED_LENGTH, -1)

    /**
      * output
      */
    val storeType: String = ConfigUtils.getString(Constants.DATA_STORE_TYPE)
    lazy val tableName: String = ConfigUtils.getString(Constants.DATA_STORE_HIVE_TABLENAME)
    lazy val outputPath: String = ConfigUtils.getString(Constants.DATA_STORE_TEXT_OUTPUT_PATH)
    lazy val outputSplit: String = ConfigUtils.getString(Constants.DATA_STORE_TEXT_COLUMNS_SPLITSTR)

    /**
      * spark 容器初始化
      */
    val sparkConf: SparkConf = new SparkConf().setAppName(String.format("BigDataLoad dataType:%s", dataType)).setMaster("local[2]")

    // 初始化spark context
    initSparkConfig(dataType, sparkConf)
    val sc = new SparkContext(sparkConf)
    val errLineAccum: Accumulator[Int] = sc.accumulator(0, "行匹配错误计数器")

    // 读取数据,并转码
    var datas: RDD[(LongWritable, String)] = sc.newAPIHadoopFile(inputPath, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf).map { case (key, value) =>
      (key, new String(value.getBytes, decode))
    }

    // 数据过滤
    if (CommonUtils.isNotBlank(lineRegex)) {
      datas = new LineFilterService().lineFilter(lineRegex, errExit, errLineAccum, datas)
    }

    // 根据分割类型，将数据进行分割成，字符串数组
    val columnValues: RDD[Array[String]] = new LineSplitService().splitText(datas, fieldSplitType, fixedLength, splitChar, regex, columnsLength)

    // 保存数据
    val saveService: DataSaveService = new DataSaveService()
    if (storeType.equalsIgnoreCase("hive")) {
      saveService.saveAsTable(dataType, tableName, sc, columnValues)
    } else {
      saveService.saveAsText(outputPath, outputSplit, columnValues)
    }

    LOG.info("errLineAccum:" + errLineAccum.value)
  }

  /**
    * 初始化Spark参数配置，也可以通过spark-submit是注入
    *
    * @param dataType  数据类型
    * @param sparkConf sparkConfig
    */
  def initSparkConfig(dataType: String, sparkConf: SparkConf): Unit = {
    // set spark参数
    if (ConfigUtils.config.hasPath("spark")) {
      val sConfig: Config = ConfigUtils.config.getConfig("spark")
      val sIterator: util.Iterator[Entry[String, ConfigValue]] = sConfig.entrySet().iterator()
      while (sIterator.hasNext) {
        val next: Entry[String, ConfigValue] = sIterator.next()
        sparkConf.set(next.getKey, next.getValue.unwrapped().toString)
      }
    }
  }

  /**
    * 初始化hadoop参数配置
    *
    * @param args     命令行参数
    * @param dataType 数据类型
    * @param config   自定义参数
    */
  def initHadoopConfig(args: Array[String], dataType: String, config: Config): Configuration = {
    /**
      * 添加hadoop配置信息
      */
    val hadoopConf = new Configuration()
    // 支持hadoop参数注入,如：-D hadoop.conf.files=xxx
    new GenericOptionsParser(hadoopConf, args)

    if (config.hasPath("hadoop")) {
      // set hadoop参数
      val hConfig: Config = config.getConfig("hadoop")
      val iterator: util.Iterator[Entry[String, ConfigValue]] = hConfig.entrySet().iterator()
      while (iterator.hasNext) {
        val next: Entry[String, ConfigValue] = iterator.next()
        hadoopConf.set(next.getKey, next.getValue.unwrapped().toString)
      }
    }

    // 设置文件名过滤器
    hadoopConf.setClass(FileInputFormat.PATHFILTER_CLASS, classOf[InputFileFilter], classOf[PathFilter])
    hadoopConf
  }

}
