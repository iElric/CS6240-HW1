package wc

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object FollowerCountMain {

  def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 2) {
      logger.error("Usage:\nwc.FollowerCountMain <input dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Word Count")
    val sc = new SparkContext(conf)

    // Delete output directory, only to ease local development; will not work on AWS. ===========
    //    val hadoopConf = new org.apache.hadoop.conf.Configuration
    //    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    //    try { hdfs.delete(new org.apache.hadoop.fs.Path(args(1)), true) } catch { case _: Throwable => {} }
    // ================
    /*val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)*/
    val textFile = sc.textFile(args(0))
    // filter the map to get the user who is being followed
    val filteredMap = textFile.flatMap(line => line.split(",")).zipWithIndex.collect {
      case (x, i) if i % 2 != 0 => x
    }
    //filteredMap.collect().foreach(println)
    val counts = filteredMap.map(user => (user, 1)).reduceByKey(_ + _)
    // help to locate string
    logger.info("Debug string:")
    logger.info(counts.toDebugString)
    counts.saveAsTextFile(args(1))
  }
}