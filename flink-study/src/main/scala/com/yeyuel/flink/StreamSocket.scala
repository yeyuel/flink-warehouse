package com.yeyuel.flink

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamSocket {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment
      .getExecutionEnvironment

    import org.apache.flink.api.scala._
    val socketStream: DataStream[String] = env
      .socketTextStream("localhost", 9999)

    val result = socketStream.flatMap(x => x.split(" "))
      .map(x => (x, 1))
      .keyBy(0)
      .sum(1)
    result.print()

    env.execute("Stream word count")
  }

}
