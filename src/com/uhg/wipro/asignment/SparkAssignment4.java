package com.uhg.wipro.asignment;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkAssignment4 {

	public static void dataFrameWithJSONfile(JavaRDD<String> longRDD, JavaSparkContext sc, SQLContext sqlContext) {
		String oddHeader = longRDD.first();
		JavaRDD<String> longRDDRecords = longRDD.filter(x -> !x.contains(oddHeader));

		SparkSession spark = SparkSession.builder().appName("Read text in DataFrames").master("local").getOrCreate();

		StructType longSchema = DataTypes
				.createStructType(new StructField[] { DataTypes.createStructField("date", DataTypes.StringType, false),
						DataTypes.createStructField("time", DataTypes.StringType, false),
						DataTypes.createStructField("size", DataTypes.IntegerType, false),
						DataTypes.createStructField("rVersion", DataTypes.StringType, false),
						DataTypes.createStructField("rArch", DataTypes.StringType, false),
						DataTypes.createStructField("rOS", DataTypes.StringType, false),
						DataTypes.createStructField("package", DataTypes.StringType, false),
						DataTypes.createStructField("version", DataTypes.StringType, false),
						DataTypes.createStructField("country", DataTypes.StringType, false),
						DataTypes.createStructField("ipId", DataTypes.StringType, false), });

		// Convert records of the people to Rows.

		JavaRDD<Row> rowRDD = longRDDRecords.map(new Function<String, Row>() {
			public Row call(String record) throws Exception {
				String[] fields = record.split(",");
				return RowFactory.create(fields[0], fields[1], Integer.parseInt(fields[2]), fields[3], fields[4],
						fields[5], fields[6], fields[7], fields[8], fields[9].trim());
			}
		});

		// Apply the schema to the RDD.
		Dataset<Row> longDF = sqlContext.createDataFrame(rowRDD, longSchema);
		
		// save data frame as json file
		longDF.write().mode("append").json("datafile/json/log_file.json");

		// read json file into data frame
		Dataset<Row> jsonDF = sqlContext.read().schema(longSchema).json("datafile/json/log_file.json");
		jsonDF.registerTempTable("Log");

		// print data frame as table
		Dataset<Row> results = sqlContext.sql("SELECT * FROM Log");

		// A groupBy date
		Dataset<Row> groupBy = jsonDF.groupBy("date").count();
		groupBy.show();

		// b. Find Max, Min and Average Download Size for each Date
		Dataset<Row> min = jsonDF.groupBy("date").min("size");
		
		// min.show();
		Dataset<Row> max = jsonDF.groupBy("date").max("size");
		
		// max.show();

		Dataset<Row> avg = jsonDF.groupBy("date").avg("size");
		// avg.show();

		// c. Sort records by Date in Ascending Order
		Dataset<Row> sort = jsonDF.sort(jsonDF.col("date").asc());
		sort.show(50);

		// d. Save the output as a JSON file
		groupBy.write().mode("append").json("datafiles/json/group_by.json");
		min.write().mode("append").json("datafiles/json/min.json");
		max.write().mode("append").json("datafiles/json/max.json");
		avg.write().mode("append").json("datafiles/json/avg.json");
		sort.write().mode("append").json("datafiles/json/sort.json");

	}

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "D:\\winutils-master\\hadoop-3.0.0");
		SparkConf conf = new SparkConf().setAppName("DataFrameWithJsonFile").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> longRDD = sc.textFile("datafile/log_file.txt");

		SQLContext sqlcontext = new org.apache.spark.sql.SQLContext(sc);

		SparkAssignment4.dataFrameWithJSONfile(longRDD, sc, sqlcontext);
	}
}
