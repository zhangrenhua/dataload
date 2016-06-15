package com.hua.spark.dataload.spark.service

import com.hua.spark.dataload.common.BigDataLoadException
import org.apache.hadoop.io.LongWritable
import org.apache.spark.Accumulator
import org.apache.spark.rdd.RDD
import org.slf4j.{Logger, LoggerFactory}

/**
  * Created by hua on 2016/4/24.
  */
class LineFilterService extends Serializable {

  private val LOG: Logger = LoggerFactory.getLogger(classOf[LineFilterService])



  /**
    * 正则过滤
    *
    * @param lineRegex    正则表达式
    * @param errLineAccum 错误计数器
    * @param datas        数据RDD
    * @return RDD
    */
  def lineFilter(lineRegex: String, errExit: Boolean, errLineAccum: Accumulator[Long], datas: RDD[(LongWritable, String)]): RDD[(LongWritable, String)] = {

    datas.filter { case (key, value) =>
      val matches: Boolean = value.matches(lineRegex)
      // 计数器累加
      if (!matches) {
        errLineAccum += 1
        LOG.warn(String.format("line:%s not matches,regex:%s", key.get().toString, lineRegex))

        if (errExit) {
          throw new BigDataLoadException("error exit.")
        }
      }
      matches
    }
  }
}
