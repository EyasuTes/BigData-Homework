import org.apache.spark.{SparkConf, SparkContext}

object SparkTop100NumWord {

  val wordPattern = "^[a-z-]{6,24}$".r
  val numberPattern = "^-?[0-9.,]{4,16}$".r

  def isValidWord(token: String): Boolean =
    wordPattern.pattern.matcher(token).matches()

  def isValidNumber(token: String): Boolean =
    numberPattern.pattern.matcher(token).matches()

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: SparkTop100NumWord <input path> <output path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark Top 100 NumWord").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))

    val tokenWindows = input
      .map(_.toLowerCase.split("[^a-z0-9.,-]+").toList)
      .flatMap(_.sliding(5))

    val numWordPairs = tokenWindows.flatMap { window =>
      for {
        i <- window.indices
        j <- window.indices
        if i != j
        t1 = window(i)
        t2 = window(j)
        if isValidNumber(t1) && isValidWord(t2)
      } yield ((t1, t2), 1)
    }.reduceByKey(_ + _)

    val top100 = numWordPairs
      .top(100)(Ordering.by(_._2))
      .map { case ((num, word), count) => s"TOP100_NUMWORD:$num:$word $count" }

    sc.parallelize(top100).saveAsTextFile(s"${args(1)}/top100numword")

    sc.stop()
  }
}
