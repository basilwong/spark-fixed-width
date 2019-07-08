package com.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.*;
import org.apache.spark.sql.functions;

public class Main {

   public static void main(String[] args) {

      // Spark Session
      SparkSession sesh = SparkSession.builder().master(args[0]).appName("test").getOrCreate();

      String schemaFilePath = args[1];
      String flatFilePath = args[2];

      FlatFileParser parser = new FlatFileParser(sesh, flatFilePath, schemaFilePath);
      Dataset<Row> methodDS = parser.getDataset(sesh);

      // Transformation
      Dataset<Row> newDS = methodDS
              .withColumn("Last-Name", functions.substring(methodDS.col("Last-Name"), 0, 10));
      newDS.show();

      // Save To Flat File
      FlatFileMaker.genFlatFile(args[3], newDS, parser.getColSizes());
   }
}