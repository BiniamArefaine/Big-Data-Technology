package edu.miu.bdt.query
import org.apache.spark.sql.SparkSession

 

object NumOfUserByLocation {
  
 
  def main(args: Array[String]){
        
   val spark = SparkSession.builder().appName("Spark Tweet Hive").master("local[*]")
      .config("hive.metastore.warehouse.uris","thrift://localhost:9083") 
      .enableHiveSupport() .getOrCreate()
      
      
      import spark.implicits._
      import spark.sql
      
      val numberOfUserLocation = sql("select Location, COUNT(userId) as userIdCount from tweets_Record_Table where Location != 'null'  GROUP BY Location ORDER BY userIdCount DESC LIMIT 10").show();
  }
}
