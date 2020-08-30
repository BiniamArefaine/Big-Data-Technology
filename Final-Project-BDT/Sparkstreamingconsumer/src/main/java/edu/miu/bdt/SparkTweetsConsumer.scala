
package edu.miu.bdt

import java.util.Properties
import java.io.File
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

object SparkTweetsConsumer {
 
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("tweets_Data").setMaster("local[*]")
   
    val sparkCont = new SparkContext(conf)
    val ssc = new StreamingContext(sparkCont, Seconds(1))
    Logger.getRootLogger().setLevel(Level.ERROR) 
    val kafkaParams = Map("metadata.broker.list" -> "localhost:9092")
    val topics = List("tweets").toSet
    val tweets = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics).map(_._2)
    .map(twt=>twt.split("\t"))
    .map(split=>(split(0).toLong,split(1),split(2).toInt,split(3),split(4).toInt,split(5),split(6).toInt,split(7).toBoolean,split(8).toBoolean,split(9).toBoolean,split(10),split(11)))
    tweets.foreachRDD((rdd,time)=>{
        
      val spark = SparkSession.builder().appName("Spark_Tweets").master("local[*]")
      .config("hive.metastore.warehouse.uris","thrift://localhost:9083") 
      .enableHiveSupport() .getOrCreate()
 
      import spark.implicits._
      import spark.sql
      
          print ("-----------here----------")

      val partitionedTweets = rdd.repartition(1).cache()
      val tweetDataFrame = partitionedTweets.
      map(t => Table_record(t._1, t._2,t._3, t._4,t._5, t._6,t._7, t._8,t._9, t._10,t._11, t._12)).toDF()
      
      val sqlContext1 = new HiveContext(spark.sparkContext)
      import sqlContext1.implicits._
      
      tweetDataFrame.write.mode(SaveMode.Append).saveAsTable("tweets_Record_Table")
      tweetDataFrame.createOrReplaceTempView("tweet_table")
      val tables = spark.sqlContext.sql("select * from tweet_table")
      
   
      println(s"========= $time =========")
    tables.show()
    })
 
    //tweets.print();
    ssc.checkpoint("OutputCheckpoint")
    ssc.start()
    ssc.awaitTermination()
  }  
}
case class Table_record(userId: Long, lang:String,friendsCount:Int, Location:String,followersCount:Int, deviceUsed:String, 
    retweetCount:Int, isSensitive:Boolean, isRetweet:Boolean, isRetweeted:Boolean, postDate:String,text:String)