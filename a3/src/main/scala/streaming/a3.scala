package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: a3 <directory>")
      System.exit(1)
    }

    // Create the context with a 3 seconds batch size
    val sparkConf = new SparkConf().setAppName("a3").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.textFileStream(args(0))
    val lowerCase = lines.map(_.toLowerCase)
    lowerCase.print()
    val noAlphaNumeric = lowerCase.map(_.replaceAll("[^a-zA-Z0-9]", ""))
    noAlphaNumeric.print()
    val removeExtraSpace = noAlphaNumeric.map(_.strip())
    removeExtraSpace.print()
    val terms = removeExtraSpace.map(_.split(" "))
    terms.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
