package com.atguigu.chapter02

import org.apache.flink.api.scala._

/**
  DataSet API
**/

object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 1. ����һ��ִ�л���
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    // 2. ��ȡ�ı��ļ�����
    val lineDataSet: DataSet[String] = env.readTextFile("input/words.txt")

    // 3. �����ݼ�����ת������
    val wordAndOne: DataSet[(String, Int)] = lineDataSet.flatMap(_.split(" ")).map((_, 1))

    // 4. ����ۺ�
    val wordAndOneGroup: GroupedDataSet[(String, Int)] = wordAndOne.groupBy(0)

    // 5. ͳ��
    val sum: AggregateDataSet[(String, Int)] = wordAndOneGroup.sum(1)

    // 6. ���
    sum.print()
  }

}
