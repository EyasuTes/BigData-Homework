import org.apache.spark.{SparkConf, SparkContext}

object SparkTop100Words {

  val wordPattern = "^[a-z-]{6,24}$".r

  def isValidWord(token: String): Boolean =
    wordPattern.pattern.matcher(token).matches()

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: SparkTop100Words <input path> <output path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark Top 100 Words").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))

    val tokens = input
      .flatMap(_.toLowerCase.split("[^a-z0-9.,-]+"))
      .filter(isValidWord)

    val top100 = tokens
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .top(100)(Ordering.by(_._2))

    val result = sc.parallelize(top100).map { case (word, count) => s"TOP100:$word $count" }

    result.saveAsTextFile(s"${args(1)}/top100words")

    sc.stop()
  }
}

