package com.atguigu.chapter02

import org.apache.flink.api.scala._

/**
  DataSet API
**/

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 1. 创建一个执行环境
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2. 读取文本文件数据
    val lineDataSet: DataSet[String] = env.readTextFile("input/words.txt")

    // 3. 对数据集进行转换处理
    val wordAndOne: DataSet[(String, Int)] = lineDataSet.flatMap(_.split(" ")).map((_, 1))

    // 4. 分组聚合
    val wordAndOneGroup: GroupedDataSet[(String, Int)] = wordAndOne.groupBy(0)

    // 5. 统计
    val sum: AggregateDataSet[(String, Int)] = wordAndOneGroup.sum(1)

    // 6. 输出
    sum.print()
  }

}
