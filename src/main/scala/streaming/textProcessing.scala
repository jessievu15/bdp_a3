package streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

object textProcessing {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: textProcessing <directory>")
      System.exit(1)
    }

    // create Spark context and Streaming with Spark configuration
    val sc = new SparkConf().setAppName("textProcessing").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(5))

    // header string
    val headerString = "query_id"
    // stop words list
    val stopWords = Set("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val cleanedline = ssc.textFileStream(args(0))
      .filter(x => !x.contains(headerString)) // remove header
      // extract the query_text column which is index 2
      .map { line =>
        val columns = line.split(",")
        // try except in case some columns have less than 4 columns
        if (columns.length >= 4) columns(2) else ""
      }

      // case fold to lower case and remove non-alphanumeric characters
      .map(_.toLowerCase())
      .map(_.replaceAll("[^a-zA-Z0-9 ]", ""))

      // tokenise to words, remove stop words, filter by length and reconstruct lines
      .map { line =>
        line.split("\\s+")
          // filter for words with more than 3 characters and non-stop words
          .filter(word => word.length >= 3 && !stopWords.contains(word))
          .mkString(" ")
      }
      // filter empty strings/rows from final output
      .filter(_.trim.nonEmpty)

    val stringCounts = cleanedline.map(x=> (x, 1))
      .reduceByKey(_ + _)
      // sort results by string counts descending then by strings in ascending alphabetical order
      .transform { x =>
        x.sortBy{ case (string, count) =>
          (-count, string)
        }
      }
    stringCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}

// spark-submit --class streaming.textProcessing --master yarn --deploy-mode client count.jar hdfs:///task1

