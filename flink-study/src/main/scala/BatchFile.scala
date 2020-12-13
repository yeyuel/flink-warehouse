import org.apache.flink.api.scala.ExecutionEnvironment

object BatchFile {
  def main(args: Array[String]): Unit = {

    val inputPath = "/home/yeyuel/git-repo/flink-warehousee/flink-study/src/main/resources/text.txt"

    val env = ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val text = env
      .readTextFile(inputPath)

    val counts = text.flatMap(x => x.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv("/home/yeyuel/git-repo/flink-warehousee/flink-study/src/main/resources/output.csv").setParallelism(1)
    env.execute("Batch WordCount")

  }

}
