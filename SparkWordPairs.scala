import org.apache.spark.{SparkConf, SparkContext}

object SparkWordPairs {

  val wordPattern = "^[a-z-]{6,24}$".r

  def isValidWord(token: String): Boolean =
    wordPattern.pattern.matcher(token).matches()

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: SparkWordPairs <input path> <output path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark Word Pairs").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))

    val tokenWindows = input
      .map(_.toLowerCase.split("[^a-z0-9.,-]+").toList)
      .flatMap(_.filter(isValidWord).sliding(5))

    val pairs = tokenWindows.flatMap { window =>
      for {
        i <- window.indices
        j <- window.indices
        if i != j
        w1 = window(i)
        w2 = window(j)
        if isValidWord(w1) && isValidWord(w2)
      } yield ((w1, w2), 1)
    }.reduceByKey(_ + _)

    val exact1000Pairs = pairs
      .filter { case (_, count) => count == 1000 }
      .map { case ((w1, w2), _) => s"EXACT1000_PAIR:$w1:$w2" }

    exact1000Pairs.saveAsTextFile(s"${args(1)}/exact1000pairs")

    sc.stop()
  }
}

