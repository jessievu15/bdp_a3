package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: a3 <input directory> <output directory> <threshold>")
      System.exit(1)
    }

    val stopWords = Set("the", "and", "for", "with", "how", "what",
      "why", "can", "you", "use", "using")

    val sparkConf = new SparkConf().setAppName("a3")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("/s4133194/checkpoint")

    // read in text file and split each document into words
    val lines = ssc.textFileStream(args(0))
    val reconstructQuery = lines.flatMap { line =>
      val fields = line.split(",", -1)
      if (fields.length < 7) None
      else {
        val userId = fields(1).trim
        val responseTime = fields(fields.length - 1).trim
        val cleanQuery = fields.slice(2, fields.length - 4).mkString(" ").toLowerCase
          .replaceAll("[^a-z0-9]+", " ")
          .trim
          .split("\\s+")
          .filter(_.length >= 3)
          .filterNot(stopWords.contains)
          .mkString(" ")
        if (cleanQuery.isEmpty) None
        else scala.util.Try(responseTime.toInt).toOption
          .map(rt => (cleanQuery, userId, rt))
      }
    }

    reconstructQuery.print()

    // Task 1
    val pairs = reconstructQuery.map {
      case (q, _, _) => (q, 1)
    }
    val batch_count = pairs.reduceByKey(_ + _)
    var task1count = 0
    batch_count.foreachRDD { batch =>
      if (!batch.isEmpty()) {
        task1count += 1
        batch.sortBy { case (q, c) => (-c, q) }
          .coalesce(1) // merge RDD's partitions into one partition
          .map { case (q, c) => s"$q,$c" }
          .saveAsTextFile(f"${args(1)}/task1-$task1count%03d")
      }
    }

    // Task 2
    val threshold = args(2).toInt
    val slow = reconstructQuery.filter {
      case (_, _, rt) => rt > threshold
    }

    val slowPairs = slow.map {
      case (q, user, rt) => (q, (user, rt))
    }

    val slow_stats = slowPairs.groupByKey().map { case (q, records) =>
      val times = records.map(_._2)
      val users = records.map(_._1).toSet
      val count = times.size
      val max_rt = times.max
      val avg_rt = times.sum.toDouble / count
      val severity = if (max_rt >= threshold * 2) "CRITICAL" else "WARNING"
      (q, count, max_rt, avg_rt, users.size, severity)
    }

    var task2count = 0
    slow_stats.foreachRDD { batch =>
      if (!batch.isEmpty()) {
        task2count += 1
        batch.sortBy { case (q, count, max_rt, _, _, severity) => (severity, -max_rt, -count, q) }
          .coalesce(1)
          .map { case (q, count, max_rt, avg_rt, users, severity) =>
            f"$q,$count,$max_rt,$avg_rt%.2f,$users,$severity"
          }
          .saveAsTextFile(f"${args(1)}/task2-$task2count%03d")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

