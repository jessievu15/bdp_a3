package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {
  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: a3 <input directory> <output directory>")
      System.exit(1)
    }

    // Create the context with a 3 seconds batch size
    val sparkConf = new SparkConf().setAppName("a3").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    // Text processing
    val lines = ssc.textFileStream(args(0))
    // Skip header
    val records = lines.filter(line =>
      !line.startsWith("query_id"))
    // Stop words set
    val stopWords = Set("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")
    val cleaned = records.flatMap { line =>
      val fields = line.split(",", -1)
      if (fields.length < 3) {
        None
      } else {
        val cleaned_query = fields(2)
          .toLowerCase
          .replaceAll("[^a-zA-Z0-9]", " ")
          .trim()
          .split("\\s+")
          .filter(term => term.length >= 3)
          .filter(term => !stopWords.contains(term))
          .mkString(" ")
        fields :+ cleaned_query
        if (cleaned_query.isEmpty) None else Some(fields :+ cleaned_query)
      }
  }

    // Task 1 - Cleaned Query Frequency Analysis
    val current_batch_count = cleaned.map(x => (x.last, 1)).reduceByKey(_ + _)
    var batchNo = 1
    current_batch_count.foreachRDD{ (rdd, time) =>
      if (!rdd.isEmpty()) {
        rdd
          // count desc, alphabetical asc
          .sortBy{case (query, count) => (-count, query)}
          .map{case (query, count) => s"$query,$count"}
          // save file as task1-001 format
          .saveAsTextFile(f"${args(1)}/task1-$batchNo%03d")
        batchNo += 1
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }

}

// hadoop fs -rm -f -r /output
// hadoop fs -mkdir /input
// hadoop fs -mkdir /output
// hadoop fs -put -f querylog_001.csv /input
// hadoop fs -put -f querylog_002.csv /input
// spark-submit --class streaming.a3 --master yarn --deploy-mode client a3.jar hdfs:///input hdfs:///output 250