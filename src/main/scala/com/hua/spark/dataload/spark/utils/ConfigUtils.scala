package com.hua.spark.dataload.spark.utils

import java.util
import java.util.Map.Entry

import com.hua.spark.dataload.common.CommonUtils
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import org.slf4j.{Logger, LoggerFactory}


/**
  * 配置文件工具类
  * Created by hua on 2016/4/7.
  */
object ConfigUtils {

  private val LOG: Logger = LoggerFactory.getLogger(ConfigUtils.getClass)
  var config: Config = null

  /**
    *
    * 初始化配置文件
    *
    * @param resourceBasename 类路径下文件名称
    */
  def load(resourceBasename: String, root: String): Unit = {
    // 加载配置文件
    config = ConfigFactory.load(resourceBasename).getConfig(root)

    LOG.info("-----------------------------所有参数-----------------------------")
    // 打印日志
    val iterator: util.Iterator[Entry[String, ConfigValue]] = config.entrySet().iterator()
    while (iterator.hasNext) {
      val next: Entry[String, ConfigValue] = iterator.next()
      LOG.info(CommonUtils.append(next.getKey, "=", next.getValue.unwrapped()))
    }
    LOG.info("------------------------------------------------------------------")
  }

  def getString(config: Config, key: String): String = {
    config.getString(key)
  }

  def getString(config: Config, key: String, default: String): String = {

    var result: String = default
    if (key != null && config.hasPath(key)) {
      result = config.getString(key)
    }
    result
  }

  def getString(key: String): String = {
    config.getString(key)
  }

  def getString(key: String, default: String): String = {

    var result: String = default
    if (key != null && config.hasPath(key)) {
      result = config.getString(key)
    }
    result
  }

  def getInt(key: String) = {
    config.getInt(key)
  }

  def getInt(key: String, default: Int): Int = {

    var result: Int = default
    if (key != null && config.hasPath(key)) {
      result = config.getInt(key)
    }
    result
  }

  def getBoolean(key: String): Boolean = {
    config.getBoolean(key)
  }

  def getBoolean(key: String, default: Boolean): Boolean = {

    var result: Boolean = default
    if (key != null && config.hasPath(key)) {
      result = config.getBoolean(key)
    }
    result
  }

}
