import org.apache.spark.{SparkConf, SparkContext}

object SparkWordCountb {

  val wordPattern = "^[a-z-]{6,24}$".r
  val numberPattern = "^-?[0-9.,]{4,16}$".r

  def isValidWord(token: String): Boolean =
    wordPattern.pattern.matcher(token).matches()

  def isValidNumber(token: String): Boolean =
    numberPattern.pattern.matcher(token).matches()

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.println("Usage: SparkWordCount <input path> <output path>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Spark Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile(args(0))

    val tokens = input
      .flatMap(_.toLowerCase.split("[^a-z0-9.,-]+"))
      .filter(token => isValidWord(token) || isValidNumber(token))

    val wordCounts = tokens
      .filter(isValidWord)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    val exact1000Words = wordCounts
      .filter { case (_, count) => count == 1000 }
      .map { case (word, _) => s"EXACT1000:$word" }

    exact1000Words.saveAsTextFile(s"${args(1)}/exact1000words")

    sc.stop()
  }
}

