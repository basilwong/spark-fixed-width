package com.example;

import org.apache.spark.sql.*;

public class Main {

   public static void main(String[] args) {

      // Spark Session
      SparkSession sesh = SparkSession.builder().master(args[0]).appName("test").getOrCreate();
      sesh.sparkContext().setLogLevel("WARN");

      // Parse Flat File
      String schemaFilePath = args[1];
      String flatFilePath = args[2];

      FlatFileParser parser = new FlatFileParser(sesh, flatFilePath, schemaFilePath);
      Dataset<Row> ds = parser.getDataset(sesh);

      // Transform
      ds = ds
              .withColumn("Birth-Date", TransUtil.toFlatFileDate(
                      ds.col("Birth-Date"),
                     "MM/dd/yyyy",
                     "yyyyMMdd"))
              .withColumn("Name", TransUtil.handleSpanish(ds.col("Name")));

      // Save To Flat File
      String outputPath = args[3];
      FlatFileGenerator.genFlatFile(sesh, schemaFilePath, outputPath, ds, true);
   }

}