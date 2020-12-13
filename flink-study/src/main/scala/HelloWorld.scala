object HelloWorld {
  def main(args: Array[String]): Unit = {
    var words = Set("hive", "hbase", "redis")
    var result = words.flatMap(x => x.toUpperCase())
    println(result)
  }

}
