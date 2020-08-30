package cs523.SparkWC;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkWordCount
{

	public static void main(String[] args) throws Exception
	{
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));
		
		// Load our input data.
		JavaRDD<String> wordLines = sc.textFile(args[0]);

		//Getting a word frequency threshold form user
		int threshold = Integer.parseInt(args[2]);
		// Calculate word count
		JavaPairRDD<String, Integer> counts = wordLines
					.flatMap(line -> Arrays.asList(line.split(" ")))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y)
		            .sortByKey();

		
		JavaRDD<String> countsByThreshold = counts
				.filter(word -> word._2 >= threshold)
				.map(w -> w._1.toLowerCase());
		
		JavaPairRDD<String, Integer> thresholdCount = countsByThreshold
				.flatMap(word -> Arrays.asList((word.split(""))))
				.mapToPair(c -> new Tuple2<String, Integer>(c, 1))
				.reduceByKey((x, y) -> x + y)
				.sortByKey();

		// Save the word count back out to a text file, causing evaluation
		counts.saveAsTextFile(args[1]+"/countedWords");
		thresholdCount.saveAsTextFile(args[1]+"/countedLetters");

		sc.close();
	}
}






