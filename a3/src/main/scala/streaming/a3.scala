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
    ssc.checkpoint("/s2113758/checkpoint")

    // read in text file and split each document into words
    val lines = ssc.textFileStream(args(0))
    val reconstructQuery = lines.flatMap { line =>
      val fields = line.split(",", -1)
      if (fields.length < 7) None
      else {
        val userId = fields(1).trim
        val responseTime = fields(fields.length - 1).trim
        val cleaned_query = fields.slice(2, fields.length - 4).mkString(" ").toLowerCase
          .replaceAll("[^a-z0-9]+", " ")
          .trim
          .split("\\s+")
          .filter(_.length >= 3)
          .filterNot(stopWords.contains)
          .mkString(" ")
        if (cleaned_query.isEmpty) None
        else scala.util.Try(responseTime.toInt).toOption
          .map(response_time => (cleaned_query, userId, response_time))
      }
    }

    // Task 1
    val pairs = reconstructQuery.map {
      case (query, _, _) => (query, 1)
    }
    val current_batch_count = pairs.reduceByKey(_ + _)
    var task1count = 0
    current_batch_count.foreachRDD { batch =>
      if (!batch.isEmpty()) {
        task1count += 1
        batch.sortBy { case (query, count) => (-count, query) }
          .coalesce(1)
          .map { case (query, count) => s"$query,$count" }
          .saveAsTextFile(f"${args(1)}/task1-$task1count%03d")
      }
    }

    // Task 2
    val threshold = args(2).toInt
    val slow = reconstructQuery.filter {
      case (_, _, response_time) => response_time > threshold
    }

    val slowPairs = slow.map {
      case (query, user, response_time) => (query, (user, response_time))
    }

    val slow_query = slowPairs.groupByKey().map { case (query, records) =>
      val times = records.map(_._2)
      val user_count = records.map(_._1).toSet
      val slow_occurrence_count = times.size
      val max_time = times.max
      val avg_time = times.sum.toDouble / slow_occurrence_count
      val severity = if (max_time >= threshold * 2) "CRITICAL" else "WARNING"
      (query, slow_occurrence_count, max_time, avg_time, user_count.size, severity)
    }

    var task2count = 0
    slow_query.foreachRDD { batch =>
      if (!batch.isEmpty()) {
        task2count += 1
        batch.sortBy { case (query, count, max_time, _, _, severity) => (severity, -max_time, -count, query) }
          .coalesce(1)
          .map { case (q, count, max_time, avg_time, users, severity) =>
            f"$q,$count,$max_time,$avg_time%.2f,$users,$severity"
          }
          .saveAsTextFile(f"${args(1)}/task2-$task2count%03d")
      }
    }

    // Task 3
    val updateFunc = (newValues: Seq[Int], oldState: Option[(Int, Int, Int)]) => {
      val current_count = newValues.sum
      val previous_count = oldState.map(_._3).getOrElse(0)
      val accumulated_count  = previous_count + current_count
      Some((current_count, previous_count, accumulated_count))
    }

    val state = current_batch_count.updateStateByKey(updateFunc)

    var task3count = 0
    state.foreachRDD { batch =>
      val newData = !batch.filter { case (_, (current_count, _, _)) => current_count > 0 }.isEmpty()

      if (newData) {
        task3count += 1

        val top10 = batch
          .sortBy { case (query, (current_count, _, accumulated_count)) => (-accumulated_count, -current_count, query) }
          .take(10)
          .map { case (query, (current_count, previous_count, accumulated_count)) =>
            val trend =
              if (previous_count == 0) "NEW"
              else if (current_count > 0) "INCREASED"
              else "UNCHANGED"
            s"$query,$current_count,$previous_count,$accumulated_count,${accumulated_count - previous_count},$trend"
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


