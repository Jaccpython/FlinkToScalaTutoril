package com.atguigu.chapter02

import org.apache.flink.streaming.api.scala._

/**
  �н���
 **/

object BoundedStreamWordCount {
  def main(args: Array[String]): Unit = {

    // 1. ����һ����ʽִ�л���
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 2. ��ȡ�ı��ļ�����
    val lineDatStream: DataStream[String] = env.readTextFile("input/words.txt")

    // 3. �����ݼ�����ת������
    val wordAndOne: DataStream[(String, Int)] = lineDatStream.flatMap(_.split(" ")).map((_, 1))

    // 4. ���յ��ʽ��з���
    val wordAndOneKey: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)

    // 5. �Է������ݽ���sum�ۺ�ͳ��
    val sum = wordAndOneKey.sum(1)

    // 6. ��ӡ���
    sum.print()

    // 7. ִ������
    env.execute()
  }

}
