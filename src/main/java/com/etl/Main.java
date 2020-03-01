package com.etl;

import org.apache.spark.sql.*;

public class Main {

   public static void main(String[] args) {

      // Spark Session
      SparkSession sesh = SparkSession.builder().master("local").appName("test").getOrCreate();
      sesh.sparkContext().setLogLevel("WARN");

      // Parse Flat File
      String schemaFilePath = "src/main/resources/schema1.csv";
      String flatFilePath = "src/main/resources/fw1.txt";

      FlatFileParser parser = new FlatFileParser(sesh, flatFilePath, schemaFilePath);
      Dataset<Row> ds = parser.getDataset(sesh);

      // Save To Flat File
      String outputPath = "src/main/resources/output";
      FlatFileGenerator.genFlatFile(sesh, schemaFilePath, outputPath, ds, true);
   }

}