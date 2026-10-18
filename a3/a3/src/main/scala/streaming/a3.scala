package streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object a3 {
  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: a3 <input directory> <output directory> <threshold>")
      System.exit(1)
    }

    // Create the context with a 3 seconds batch size
    val sparkConf = new SparkConf().setAppName("a3")
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("/s2113758/checkpoint")

    // Text processing
    val lines = ssc.textFileStream(args(0))
    // Skip header
    val records = lines.filter(line => !line.startsWith("query_id"))
    // Stop words set
    val stopWords = Set("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")
    val cleaned = records.flatMap { line =>
      val fields = line.split(",", -1)
      if (fields.length < 7) {
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
        if (cleaned_query.isEmpty) None else Some(fields :+ cleaned_query)
      }
    }

    // Task 1 - Cleaned Query Frequency Analysis
    val current_batch_count = cleaned.map(x => (x.last, 1)).reduceByKey(_ + _)
    var batchNo1 = 1
    current_batch_count.foreachRDD{ (rdd, _) =>
      if (!rdd.isEmpty()) {
        rdd
          // count desc, alphabetical asc
          .sortBy{case (query, count) => (-count, query)}
          .map{case (query, count) => s"$query,$count"}
          // save file as task1-001 format
          .saveAsTextFile(f"${args(1)}/task1-$batchNo1%03d")
        batchNo1 += 1
      }
    }

    // Task 2 - Slow Query Incident Monitoring
    val threshold = args(2).toLong
    val slow_records = cleaned.flatMap{ record =>
      val response_time = record(6).trim.toLong
      if (response_time > threshold) {
        Some((record.last, (response_time, record(1)))) // (cleaned_query, (response_time, user_id))
      } else {
        None
      }
    }

    val slow_query = slow_records.groupByKey().map { case (query, time_user) =>
      val times = time_user.map(_._1) // pull out 1st element of the time_user tuple = response_time element
      val slow_occurrence_count = times.size
      val max_time = times.max
      val avg_time = times.sum.toDouble / slow_occurrence_count
      val user_count = time_user.map(_._2).toSet.size // to.Set removes duplicate user
      val severity = if (max_time >= threshold * 2) "CRITICAL" else "WARNING"
      (query, slow_occurrence_count, max_time, avg_time, user_count, severity)
    }

    var batchNo2 = 1
    slow_query.foreachRDD{ (rdd, _) =>
      if (!rdd.isEmpty()) {
        rdd
          .sortBy{case (query, slow_occurrence_count, max_time, _, _, severity) =>
            (severity, -max_time, -slow_occurrence_count, query)}
          .map{case (query, slow_occurrence_count, max_time, avg_time, user_count, severity) =>
            f"$query,$slow_occurrence_count,$max_time,$avg_time%.2f,$user_count,$severity"}
          .saveAsTextFile(f"${args(1)}/task2-$batchNo2%03d")
        batchNo2 += 1
      }
    }

    // Task 3: Advanced Stateful Trending Query Analysis
    val updateFunc = (newValues: Seq[Int], oldState: Option[(Int, Int, Int)]) => {
      val current_count = newValues.sum
      val previous_count = oldState.map(_._3).getOrElse(0)
      val accumulated_count  = previous_count + current_count
      Some((current_count, previous_count, accumulated_count))
    }

    val state = current_batch_count.updateStateByKey(updateFunc)

    var BatchNo3 = 0
    state.foreachRDD { batch =>
      val newData = !batch.filter { case (_, (current_count, _, _)) => current_count > 0 }.isEmpty()

      if (newData) {
        BatchNo3 += 1

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
          .saveAsTextFile(f"${args(1)}/task3-$BatchNo3%03d")
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