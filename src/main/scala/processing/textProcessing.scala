package processing

import org.apache.spark.{SparkConf, SparkContext}

object textProcessing {
  def main(args: Array[String]) {
    // create Spark context with Spark configuration
    val sc = new SparkContext(new SparkConf().setAppName("textProcessing"))

    // get threshold - a3 threshold is 250
    // val threshold = args(1).toInt

    // read in text file and split each document into words
    // val tokenized = sc.textFile(args(0)).flatMap(_.split(","))

    // read csv file in using textFile()
    val text = sc.textFile("hdfs:///querylog_001_spaces.csv")

    // extract the query_text column which is index 2
    val queryText = text.map(line => {
      val columns = line.split(",")
      // try except in case some columns have less than 4 columns
      if (columns.length >= 4) {
        columns(2)
      } else {
        ""
      }
    })

    // case fold to lower case
    val lowercase = queryText.map(x => x.toLowerCase())

    // remove non-alphanumeric characters
    val alphaOnly = lowercase.map(x => x.replaceAll("[^a-zA-Z0-9 ]", ""))

    // stop words list
    val stopWords = Set("the", "and", "for", "with", "how", "what", "why", "can", "you", "use", "using")

    val reconstructedQuery = alphaOnly
      .filter(line => line != "queryText" && line.nonEmpty)
      .map(line => {
        // remove extra spaces by splitting on one or more spaces
        val words = line.split("\\s+")

        // filter for words with more than 3 characters and non-stop words
        val filteredWords = words
          .filter(word => word.length >= 3)
          .filter(word => !stopWords.contains(word))

        // reconstruct the cleaned query
        filteredWords.mkString(" ")
      })
      // Ignore empty rows
      .filter(_.nonEmpty)



