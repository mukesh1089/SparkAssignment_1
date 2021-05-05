package com.uhg.wipro.asignment;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkAssignment1 {

	// Filter out records where package = “NA” and version = “NA”

	public static void filterRecords(JavaRDD<String> longRDD) {
		String oddHeader = longRDD.first();
		JavaRDD<String> longRDDRecords = longRDD.filter(x -> !x.contains(oddHeader));

		JavaRDD<String> filteredRecords = longRDDRecords.filter(record -> {
			String[] close = record.split(",");
			String pkg = close[6];
			String version = close[7];
			if (pkg.equals("NA") && version.equals("NA")) {
				return true;
			} else {
				return false;
			}
		});
		filteredRecords.foreach(records -> System.out.println("Filtered Records====>>" + records));
	}

	// Find total number of downloads for each package.

	public static void findTotalNumberOfDownloads(JavaRDD<String> longRDD) {
		JavaPairRDD<String, Long> downloads = longRDD.mapToPair(rowValue -> {
			String[] columns = rowValue.split(",");
			String pkg = columns[6];

			return new Tuple2<String, Long>(pkg, 1L);
		});
		JavaPairRDD<String, Long> totalCount = downloads.reduceByKey((value1, value2) -> value1 + value2);
		totalCount.foreach(tuple -> System.out.println(tuple._1 + " has " + tuple._2 + " downloads"));
	}

	// Find Average download size for each Country

	public static void findAverageOfEachCountry(JavaRDD<String> longRDD) {
		String oddHeader = longRDD.first();
		JavaRDD<String> longRDDRecords = longRDD.filter(x -> !x.contains(oddHeader));

		JavaPairRDD<String, Long> downloads = longRDDRecords.mapToPair(rowValue -> {
			String[] columns = rowValue.split(",");
			String size = columns[2];
			String country = columns[8];

			return new Tuple2<String, Long>(country, Long.valueOf(size).longValue());
		});

		Map<String, Long> totalCount = downloads.countByKey();

		JavaPairRDD<String, Long> totalSum = downloads.reduceByKey((value1, value2) -> (value1 + value2));
		totalSum.foreach(tuple -> System.out
				.println(tuple._1 + " has " + tuple._2 / totalCount.get(tuple._1) + " average of downloads"));
	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\winutils-master\\hadoop-3.0.0");

		SparkConf conf = new SparkConf().setAppName("SparkAssignment2").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> longRDD = sc.textFile("datafile/log_file.txt");
		SparkAssignment1.filterRecords(longRDD);
		SparkAssignment1.findTotalNumberOfDownloads(longRDD);
		SparkAssignment1.findAverageOfEachCountry(longRDD);

		sc.close();
	}

}
