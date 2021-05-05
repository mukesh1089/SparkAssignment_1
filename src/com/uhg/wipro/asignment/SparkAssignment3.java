package com.uhg.wipro.asignment;

import org.apache.derby.iapi.types.DataType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkAssignment3 {
	
	//Spark SQL Assignments
	
	public static void dataFrameDownload(JavaRDD<String> longRDD,JavaSparkContext sc,SQLContext sqlContext)
	{
		String oddHeader=longRDD.first();
		JavaRDD<String> longRDDRecord=longRDD.filter(x->!x.contains(oddHeader));
		
		
		SparkSession spark=SparkSession.builder()
				.appName("Read text in Data frames")
				.master("local").getOrCreate();
		
		 StructType logSchema = DataTypes.createStructType(new StructField[] {
	                DataTypes.createStructField("date", DataTypes.StringType, false),
	                DataTypes.createStructField("time", DataTypes.StringType, false),
	                DataTypes.createStructField("size", DataTypes.StringType, false),
	                DataTypes.createStructField("rVersion", DataTypes.StringType, false),
	                DataTypes.createStructField("rArch", DataTypes.StringType, false),
	                DataTypes.createStructField("rOS", DataTypes.StringType, false),
	                DataTypes.createStructField("package", DataTypes.StringType, false),
	                DataTypes.createStructField("version", DataTypes.StringType, false),
	                DataTypes.createStructField("country", DataTypes.StringType, false),
	                DataTypes.createStructField("ipId", DataTypes.StringType, false),
	        });
		// Convert records of the RDD (people) to Rows.
	        JavaRDD<Row> rowRDD = longRDDRecord.map(
	          new Function<String, Row>() {
	            public Row call(String record) throws Exception {
	              String[] fields = record.split(",");
	              return RowFactory.create(fields[0], fields[1], fields[2], fields[3], fields[4], fields[5], fields[6], fields[7], fields[8], fields[9].trim());
	            }
	          });
	        // Apply the schema to the RDD.
	        Dataset<Row> logDF = sqlContext.createDataFrame(rowRDD, logSchema);
	        
	        // Register the DataFrame as a table.
	        logDF.registerTempTable("Log");
	        
	        //print data frame as table
	        Dataset<Row> results = sqlContext.sql("SELECT * FROM Log");
	        //results.printSchema();
	        results.show(5);
	        
	        Dataset<Row> newDF = logDF.withColumn("Download_Type", functions.when((logDF.col("size").$less(100000)), "Small")
	                .when(logDF.col("size").$greater(100000).and(logDF.col("size").$less(1000000)), "Medium")
	                  .otherwise("Large"));
	        //newDF.printSchema();
	        newDF.show(5);
	        
	        //Find the total number of Downloads by Country and Download Type
	        Dataset<Row> totalDownloadDF = newDF.groupBy("country", "Download_Type").count();
	        totalDownloadDF.show();
	        
	        //e.    Save output as a Parquet File
	        totalDownloadDF.write().format("parquet").saveAsTable("logSummary");
	    }
	    
	    

	 

	    public static void main(String[] args) {
	    	System.setProperty("hadoop.home.dir", "D:\\winutils-master\\hadoop-3.0.0");

	 

	        SparkConf conf = new SparkConf().setAppName("SparkAssignment3").setMaster("local[*]");

	 

	        JavaSparkContext sc = new JavaSparkContext(conf);

	 

	        JavaRDD<String> logRdd = sc.textFile("datafile/log_file.txt");
	        
	        // sc is an existing JavaSparkContext.
	        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);
	        
	        SparkAssignment3.dataFrameDownload(logRdd, sc, sqlContext);
	        
	        sc.close();
	        
	}
}
