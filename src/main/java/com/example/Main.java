package com.example;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

public class Main {

   public static void main(String[] args) {

      // Spark Session
      SparkSession sesh = SparkSession.builder().master(args[0]).appName("test").getOrCreate();
      sesh.sparkContext().setLogLevel("WARN");

      // Parse Flat File
      String schemaFilePath = args[1];
      String flatFilePath = args[2];

      FlatFileParser parser = new FlatFileParser(sesh, flatFilePath, schemaFilePath);
      Dataset<Row> methodDS = parser.getDataset(sesh);

      // Transformation
//      for (String c : methodDS.columns()) {
//         methodDS = methodDS
//                 .withColumn(c, TransUtil.set_col_length(methodDS.col(c), 3));
//      }
      methodDS.show();
      methodDS = methodDS
              .withColumn("Birth-Date", TransUtil.toFlatFileDate(methodDS.col("Birth-Date")))
              .withColumn("Name", TransUtil.handleSpanish(methodDS.col("Name")));
      methodDS.show();

      // Save To Flat File
      String outputPath = args[3];
      FlatFileMaker.genFlatFile(sesh, schemaFilePath, outputPath, methodDS, true);
   }

}