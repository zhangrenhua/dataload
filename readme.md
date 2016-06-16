---
title: Spark数据加载 To (HDFS/Hive)
description: 
tags: [hadoop生态圈, spark, scala]
categories: spark
date: 2016-4-19 20:40:16
---

## 背景

最近由于项目需要，利用Spark加载数据到HDFS/Hive表中，传统做法可以使用Pig或者自己编写MapReduce。本来打算使用Pig，由于Cloudera5.3中的Pig运行驱动还是MapReduce(最新版本已经支持Spark)，并且来源数据格式千奇百怪，定长、首行有注释、多字符分割、文件编码、数据类型多等问题，所以没有使用Pig。当然这些问题都可以通过Pig提供的自定义UDF来解决，一些标准的数据加工处理，还是建议使用Pig，毕竟PigLatin语法的确很丰富。

笔者使用Spark1.6.0，Spark升级尤为频繁，每次都修复大量bug，所以选择新版本。
本人第一次编写scala程序，望大家多多指教。


## 项目功能

本项目采用Spark + Spark Sql，主要用来，将一些常用格式的数据加工到Hive或者HDFS中。其中功能细节如下：

**数据层面**
1、文件名正则匹配
2、文件读取跳过前面几行
3、文件内容编码转换
4、按行或者固定分割符读取每行记录


**文件解析**
1、字段定长分割
2、字段，单/多字符分割
3、根据正则表达式解析字段

**文件存储**
1、自动数据类型转换，存入Hive
2、字段拼接，存入HDFS


**待开发**
1、字段过滤器（简单的过滤规则）
2、增加数据存储之前/后的一些SQL执行
3、支持更多的数据类型
4、更多类型的字段格式化

## 配置项说明

配置示例：
```
demo {

  // hadoop 参数配置
  hadoop {
    mapreduce.input.fileinputformat.input.dir.recursive = true // 递归
    input.file.filter = ".*" // 文件正则
    skip.header.line.count = 0 //跳过文件前几行
  }

  // spark 参数配置
  spark {
    spark.serializer = "org.apache.spark.serializer.KryoSerializer"
  }

  data.input.path = "/tmp/input/demo" // 数据目录
  file.text.decode = "UTF-8" // 字符解码
  data.input.line.regex.str = ".*" // 行的正则匹配表达式，可以为空
  data.input.line.regex.err.exit = false // 正则不匹配则异常退出


  columns.split.type = "regex" // 字段分割类型，fixed:定长,splitstr:字符分割,regex:正则分割
  // 字符分割，参数
  columns.split.splitstr {
    str = "\\|" // 字段分隔符
  }
  // 定长分割，参数
  columns.split.fixed {
    columns.length = "1,1,3,1,5,4,10,23" // 字段定长分割，如：10,12,34
  }
  // 正则表达式分割，参数
  columns.split.regex {
    // 正则表达式
    regex.str = "(\\w+)\\|(\\d+)\\|([+-]?\\d*\\.\\d+)\\|(\\d+)\\|([+-]?\\d*\\.\\d+)\\|(\\w+)\\|([\\d\\s-]+)\\|([\\d-\\s:\\.]+)"
  }

  columns.fixed.length = -1 // 字段长度，用于分割后校验,-1:不校验

  columns.format {
    date.6 = "yyyy-MM-dd" // 日期格式化,字段序号
    date.7 = "yyyy-MM-dd HH:mm:ss.S" // 日期格式化,字段序号
  }


  data.store.type = "hive" // 数据存储类型,hive：存入hive表，text:数据目录
  // 存入hive
  data.store.hive.tablename = "default.demo" // 类型为hive：表名

  // 文本输出
  data.store.text.output.path = "/tmp/output/demo" // 类型为text：输出目录
  data.store.text.columns.splitstr = "&" // 存储的分割符
}
```

参数说明：
`columns.split.type`：字段分割类型，fixed:定长,splitstr:字符分割,regex:正则分割</br>
`data.store.type`：控制数据存储类型,hive：存入hive表，text:数据目录

## 测试

### hive建表
```
DROP TABLE
    demo;
CREATE TABLE
    demo
    (
        a string,
        b INT,
        c FLOAT,
        d bigint,
        e DECIMAL(10,2),
        f BOOLEAN,
        g DATE,
        h TIMESTAMP
    );

DROP TABLE
    demo;
CREATE TABLE
    demo
    (
        a string,
        b INT,
        c FLOAT,
        d bigint,
        e DECIMAL(10,2),
        f BOOLEAN,
        g DATE,
        h TIMESTAMP
    )
    stored as orc;
```
parquet 不支持date类型,所以没测试。

### 测试数据

字符/正则分割，测试数据(data.txt)：
```
a|1|1.1|2|12.56|true|2016-04-24|2016-04-24 12:12:12.123
b|2|1.2|3|12.57|false|2016-04-24|2016-04-24 12:12:12.124
c|3|1.3|4|12.58|false|2016-04-24|2016-04-24 12:12:12.125
```
定长分割，测试数据(data1.txt):
```
a11.1212.56true2016-04-242016-04-24 12:12:12.123
b21.2312.57false2016-04-242016-04-24 12:12:12.124
c31.3412.58false2016-04-242016-04-24 12:12:12.125
```

### 运行测试

设置本地运行模式setMaster("local[2]")，运行DataLoad.scala即可。

测试结果：</br>
![Alt text](http://7xoqbc.com1.z0.glb.clouddn.com/hadoop-spark-sql-demo-sel.png)


本次只是简单的功能测试，后续我会加入大数据量测试结果。


## apache访问日志测试

### 建表

DROP TABLE
    APACHE_LOG_BASE;
CREATE TABLE
    APACHE_LOG_BASE
    (
        IP STRING COMMENT '访问IP',
        UNIQUEID STRING COMMENT '浏览者标识',
        AUTHENINFORMATION STRING COMMENT '验证信息',
        REQ_DATETIME STRING COMMENT '请求时间',
        RESPON_DURATION STRING COMMENT '响应时间',
        RESOURCE STRING COMMENT '方法资源协议',
        ACCESS_STATUS STRING COMMENT '状态代码',
        FLOW_VALUE STRING COMMENT '流量',
        URL STRING COMMENT '请求URL',
        BROWSER_TYPE STRING COMMENT '浏览器标识'
    )stored AS parquet;


### 数据文件

样式：
```
121.239.44.41 - - [01/Jul/2015:08:02:52 +0800] 0 "GET /pweb/js/ui/init.js?version=1.1 HTTP/1.0" 301 272 "https://ebanks.xxx.com/pweb/InlineTransferPre.do?_RestoreFromQueue=true&_QueueName=PageRoot&_QueueLevel=1" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)"
218.82.139.244 - - [01/Jul/2015:08:02:52 +0800] 0 "GET /pweb/zh_CN/default/images/menubg.gif HTTP/1.0" 301 278 "https://ebanks.xxx.com/pweb/prelogin.do?LoginType=R&_locale=zh_CN" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729)"
121.239.44.41 - - [01/Jul/2015:08:02:52 +0800] 0 "GET /pweb/script.do HTTP/1.0" 200 35965 "https://ebanks.xx.com/pweb/InlineTransferPre.do?_RestoreFromQueue=true&_QueueName=PageRoot&_QueueLevel=1" "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0)"
101.229.155.246 - - [01/Jul/2015:08:02:52 +0800] 0 "GET /pweb/zh_CN/default/images/login_up.png HTTP/1.0" 301 280 "https://ebanks.xx.com/pweb/prelogin.do?LoginType=R&_locale=zh_CN" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)"
101.229.155.246 - - [01/Jul/2015:08:02:52 +0800] 0 "GET /pweb/zh_CN/default/images/titbg_02_center.gif HTTP/1.0" 301 287 "https://ebanks.xx.com/pweb/prelogin.do?LoginType=R&_locale=zh_CN" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/7.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)"
```

由于数据涉及隐私所以没全部拿出，下图是我用text格式保存时的数据大小：
![Alt text](http://7xoqbc.com1.z0.glb.clouddn.com/hadoop-spark-dataload-test5.png)


### 配置文件
```
demo {

  // hadoop 配置信息
  hadoop {
    mapreduce.input.fileinputformat.input.dir.recursive = true // 递归
    input.file.filter = ".*" // 文件正则
    skip.header.line.count = 0 //跳过文件前几行
  }

  // spark 配置信息
  spark {
    spark.serializer = "org.apache.spark.serializer.KryoSerializer"
  }

  data.input.path = "/tmp/dataload" // 数据目录
  file.text.decode = "UTF-8" // 字符解码
  data.input.line.regex.str = ".*" // 行的正则匹配表达式，可以为空
  data.input.line.regex.err.exit = false // 正则不匹配则异常退出


  columns.split.type = "regex" // 字段分割类型，fixed:定长,splitstr:字符分割,regex:正则分割
  // 字符分割，参数
  columns.split.splitstr {
    str = "\\|" // 字段分隔符
  }
  // 定长分割，参数
  columns.split.fixed {
    columns.length = "1,1,3,1,5,4,10,23" // 字段定长分割，如：10,12,34
  }
  // 正则表达式分割，参数
  columns.split.regex {
    // 正则表达式
    regex.str = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w\\d:/]+\\s[+\\-]\\d{4})\\] (\\d+) \"(.+?)\" (\\d{3}) (\\d+?) \"([^\"]+)\" \"([^\"]+)\".*"
  }

  columns.fixed.length = -1 // 字段长度，用于分割后校验,-1:不校验

  columns.format {
    // date.6 = "yyyy-MM-dd" // 日期格式化,字段序号
    // date.7 = "yyyy-MM-dd HH:mm:ss.S" // 日期格式化,字段序号
  }


  data.store.type = "hive" // 数据存储类型,hive：存入hive表，text:数据目录
  // 存入hive
  data.store.hive.tablename = "test.apache_log_base" // 类型为hive：表名

  // 文本输出
  data.store.text.output.path = "/tmp/output/demo" // 类型为text：输出目录
  data.store.text.columns.splitstr = "&" // 存储的分割符
}
```

如果你的apache日志格式不是这样可以修改 regex.str正则表达式。


### 运行测试

执行脚本:
```
spark-submit --driver-memory 1g --driver-java-options -XX:MaxPermSize=128m \
    --executor-cores 3 --num-executors 2  \
    --jars /home/hdper/spark/lib/datanucleus-api-jdo-3.2.6.jar,/home/hdper/spark/lib/datanucleus-core-3.2.10.jar \
    --files /home/hdper/appstest/zhangrenhua/dataload/demo.conf,/etc/hive/conf/hive-site.xml --class com.hua.spark.dataload.DataLoad \
    --master yarn-cluster dataload-1.0-SNAPSHOT.jar demo demo.conf
```

由于测试集群有点问题，所以只开2个executor,每个executor使用3个cpu并行执行，执行截图：

![Alt text](http://7xoqbc.com1.z0.glb.clouddn.com/hadoop-spark-dataload-test1.png)
spark task

![Alt text](http://7xoqbc.com1.z0.glb.clouddn.com/hadoop-spark-dataload-test2.png)
spark task detail

![Alt text](http://7xoqbc.com1.z0.glb.clouddn.com/hadoop-spark-dataload-test3.png)
spark-submit success

![Alt text](http://7xoqbc.com1.z0.glb.clouddn.com/hadoop-spark-dataload-test4.png)
yarn detail