package com.atguigu.chapter02

import org.apache.flink.streaming.api.scala._

/**
  有界流
 **/

object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {

    // 1. 创建一个流式执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. 读取文本文件数据
    val lineDatStream: DataStream[String] = env.readTextFile("input/words.txt")

    // 3. 对数据集进行转换处理
    val wordAndOne: DataStream[(String, Int)] = lineDatStream.flatMap(_.split(" ")).map((_, 1))

    // 4. 按照单词进行分组
    val wordAndOneKey: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)

    // 5. 对分组数据进行sum聚合统计
    val sum = wordAndOneKey.sum(1)

    // 6. 打印输出
    sum.print()

    // 7. 执行任务
    env.execute()
  }

}
