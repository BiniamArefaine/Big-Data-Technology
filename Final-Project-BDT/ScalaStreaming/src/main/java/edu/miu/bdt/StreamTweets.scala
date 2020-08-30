
package edu.miu.bdt

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.auth.OAuthAuthorization
import twitter4j.conf.ConfigurationBuilder
import org.apache.log4j.{Level, Logger}

/** listening to a stream of Tweets*/
object StreamTweets {
 
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("tweets_Data").setMaster("local[*]")
    val saprkContext  = new SparkContext(conf)
    val ssc = new StreamingContext(saprkContext, Seconds(1))
    Logger.getRootLogger().setLevel(Level.ERROR)
    
    // values of Twitter API.
    val consumerKey = "VOeWneeOyfep5Z7KECgoo1r8B" //---------------------------------------------------ConsumerKey
    val consumerAPISecret = "Er3J5vZ0Ynfv5E27l6Is2M6UdYV2bxjcx2c4uflZF7eBje12sB" //-----------------------API secret
    val accessToken ="1291753211309821954-HuPCBjtDpBjo3aBRGjyeenwFXdKNFn" //----------------------------Access token
    val accessTokenSecret = "XPKUov1kgZ3yGzGxvlC9ituok0SVgAwjeGToNJ9WBdubl" //------------------------ Token secret

    //Connection to Twitter API
    val configurationBuilder = new ConfigurationBuilder
    configurationBuilder.setDebugEnabled(true).setOAuthConsumerKey(consumerKey).setOAuthConsumerSecret(consumerAPISecret).setOAuthAccessToken(accessToken).setOAuthAccessTokenSecret(accessTokenSecret)

    val auth = new OAuthAuthorization(configurationBuilder.build)
    val tweets = TwitterUtils.createStream(ssc, Some(auth))
   
    val tweet_Status = tweets.map(status => (status.getUser.getId, status.getLang,status.getUser.getFriendsCount, 
      status.getUser.getLocation,status.getUser.getFollowersCount,status.getSource.split("[<,>]")(2),
      status.getRetweetCount,status.isPossiblySensitive,status.isRetweet,status.isRetweeted,status.getCreatedAt.toString,
      status.getText)).map(_.productIterator.mkString("\t"))
      
      tweet_Status.print()
    
    tweet_Status.foreachRDD { (rdd, time) => rdd.foreachPartition { partitionIter => val props = new Properties()
         
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("bootstrap.servers", "localhost:9092")
        val producer = new KafkaProducer[String, String](props)
        partitionIter.foreach { elem =>
          val dat = elem.toString()
          val data = new ProducerRecord[String, String]("tweets", null, dat)
          producer.send(data)
        }
           
        producer.flush()
        producer.close()
      }
    }
    
    
    ssc.start()
    ssc.awaitTermination()
    
  }  
}
