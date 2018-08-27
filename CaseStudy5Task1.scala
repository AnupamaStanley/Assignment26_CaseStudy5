import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.log4j.{Level,Logger}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
object CaseStudy5Task1{

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("TextFileStream").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(10))
    val lines = ssc.textFileStream("/home/acadgild/casestudy5")
    val wordcounts = lines.flatMap(line => line.split(" ").map(word =>(word,1))).reduceByKey(_ + _)
    wordcounts.print()
    ssc.start()
    ssc.awaitTermination()

  }
}