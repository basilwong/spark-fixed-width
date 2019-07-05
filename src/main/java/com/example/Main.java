package com.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import java.util.*;
import org.apache.spark.sql.functions;
import com.example.FlatFileParser;

import javax.swing.text.Element;
import org.apache.commons.lang3.StringUtils;


public class Main {


   public static String rowToFWSTring(List<Integer> rowSize, Row r) {
      String[] vals = new String[r.size()];
      for (int i = 0; i < r.size(); i++) {
         vals[i] = StringUtils.leftPad(r.getString(i), rowSize.get(i), ' ');
      }
      return String.join("", vals);
   }

   public static void main(String[] args) {

      // Spark Session
      System.out.println("Creating Spark Session...");
      SparkSession sesh = SparkSession.builder().master("local").appName("test").getOrCreate();
      System.out.println("Spark Session Created.");

      String schemaFilePath = "src/main/resources/schema1.csv";
      String flatFilePath = "src/main/resources/fw1.txt";

      FlatFileParser parser = new FlatFileParser(sesh, flatFilePath, schemaFilePath);
      Dataset<Row> methodDS = parser.getDataset(sesh);

      // Transformation
      Dataset<Row> newDS = methodDS
              .withColumn("Last-Name", functions.substring(methodDS.col("Last-Name"), 0, 10));
      newDS.show();

      // Save To Flat File
      FlatFileMaker.genFlatFile(newDS, parser.getColSizes());
   }
}
