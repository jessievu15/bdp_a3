package org.example

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object QueryLogAnalysis {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: QueryLogAnalysis <directory>")
      System.exit(1)
    }

    // Create the context with a 3 seconds batch size
    val sparkConf = new SparkConf().setAppName("SocketWordCount").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val lines = ssc.textFileStream(args(0))
    val lowercase = lines.toLowerCase

  }

}
