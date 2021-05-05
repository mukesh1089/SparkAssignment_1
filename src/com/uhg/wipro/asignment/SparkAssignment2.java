package com.uhg.wipro.asignment;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

public class SparkAssignment2 {

	// Create a Scala hashmap with country codes and country names for all country
	// codes available in the log_file

	public static Map<String, String> getCountryCodeNames(JavaRDD<String> longRDD) {
		String oddHeader = longRDD.first();
		JavaRDD<String> longRDDRecords = longRDD.filter(x -> !x.contains(oddHeader));

		JavaPairRDD<String, String> countryCodeAndNames = longRDDRecords.mapToPair(rowValue -> {
			String[] columns = rowValue.split(",");
			String countryName = columns[8];
			String countryCode = columns[9];

			return new Tuple2<String, String>(countryName, countryCode);
		});

		Map<String, String> countryMap = countryCodeAndNames.collectAsMap();
		countryMap.forEach((countryCode, countryName) -> System.out.println(countryCode + " : " + countryName));

		return countryMap;
	}

	// Perform a broadcast join to generate the below output using RDD API Country
	// Code, Country Name, Total Downloads
	// And save output as text file
	public static void performBroadcastJoin(JavaRDD<String> longRDD, JavaSparkContext sc) {
		String oddHeader = longRDD.first();
		JavaRDD<String> longRDDRecords = longRDD.filter(x -> !x.contains(oddHeader));
		JavaPairRDD<String, Long> downloads = longRDDRecords.mapToPair(rowValue -> {
			String[] columns = rowValue.split(",");
			String countryCode = columns[8];

			return new Tuple2<String, Long>(countryCode, 1L);
		});

		JavaPairRDD<String, Long> totalCount = downloads.reduceByKey((value1, value2) -> value1 + value2);
		// totalCount.foreach(tuple -> System.out.println(tuple._1 +" has "+ tuple._2 +"
		// downloads"));

		Map<String, String> countryMap = getCountryCodeNames(longRDD);
		HashMap<String, String> countryHashMap = new HashMap<String, String>(countryMap);

		Broadcast<HashMap<String, String>> prodBC = sc.broadcast(countryHashMap);

		JavaRDD<Country> countryBroadcastJoinRDD = totalCount.map(rec -> {
			String countryName = rec._1;
			String countryCode = prodBC.getValue().get(rec._1);
			Long totalDownloads = rec._2;

			return new Country(countryCode, countryName, totalDownloads);
		});

		countryBroadcastJoinRDD.foreach(rec -> System.out.println("====>>" + rec.toString()));
		countryBroadcastJoinRDD.saveAsTextFile("datafile/output/log_file.txt");

	}

	public static void main(String[] args) {

		System.setProperty("hadoop.home.dir", "D:\\winutils-master\\hadoop-3.0.0");

		SparkConf conf = new SparkConf().setAppName("SparkAssignment2").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> longRDD = sc.textFile("datafile/log_file.txt");

		SparkAssignment2.getCountryCodeNames(longRDD);

		SparkAssignment2.performBroadcastJoin(longRDD, sc);

		sc.close();
	}

}
