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

    // Task 3
    val updateFunc = (newValues: Seq[Int], oldState: Option[(Int, Int, Int)]) => {
      val curr_count = newValues.sum
      val prev_count = oldState.map(_._3).getOrElse(0)
      val acc_count  = prev_count + curr_count
      Some((curr_count, prev_count, acc_count))
    }

    val state = batch_count.updateStateByKey(updateFunc)

    var task3count = 0
    state.foreachRDD { batch =>
      val newData = !batch.filter { case (_, (curr, _, _)) => curr > 0 }.isEmpty()

      if (newData) {
        task3count += 1

        val top10 = batch
          .sortBy { case (q, (curr, _, acc)) => (-acc, -curr, q) }
          .take(10)
          .map { case (q, (curr, prev, acc)) =>
            val trend =
              if (prev == 0) "NEW"
              else if (curr > 0) "INCREASED"
              else "UNCHANGED"
            s"$q,$curr,$prev,$acc,${acc - prev},$trend"
          }

        batch.sparkContext
          .parallelize(top10, 1)
          .saveAsTextFile(f"${args(1)}/task3-$task3count%03d")
      }
    }
      ssc.start()
      ssc.awaitTermination()
  }
}


