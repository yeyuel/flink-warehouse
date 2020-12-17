package com.yeyuel.flink

import java.nio.file.Paths

import org.apache.flink.api.scala.ExecutionEnvironment

object BatchFile {
  def main(args: Array[String]): Unit = {

    val source = getClass.getResource("").getPath

    println(source)
    val inputPath = Paths.get(source, "text.txt").toString
    val outputPath = Paths.get(source, "output.txt").toString

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val text = env
      .readTextFile(inputPath)

    val counts = text.flatMap(x => x.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv(outputPath).setParallelism(1)
    env.execute("Batch WordCount")

  }

}
