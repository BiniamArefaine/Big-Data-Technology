package edu.miu.bdt.query
import org.apache.spark.sql.SparkSession

 

object QueryFriendCount {
  
  def main(args: Array[String]){
        
   val spark = SparkSession.builder().master("local[*]")
      .config("hive.metastore.warehouse.uris","thrift://localhost:9083") 
      .enableHiveSupport() .getOrCreate()
      
      
      import spark.implicits._
      import spark.sql
      
      val QueryFriendsCount = sql("select userId, friendscount from tweets_Record_Table order by friendscount DESC Limit 10").show();
      
  }
}