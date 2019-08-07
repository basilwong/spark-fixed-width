package com.etl;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;

import java.util.*;

public interface FlatFileWarlock {

    public String run(SparkSession sp, String inputPath, String flatFilePath);

    Dataset<Row> readInput(SparkSession sp, String inputPath);

    Dataset<Row> transformDS(Dataset<Row> ds);

    JavaRDD<String> prepData(SparkSession sp, Dataset<Row> ds);

    String createFlatFile(JavaRDD<String> jr, String flatFilePath);

    HashMap<String, Integer> getSchema();
}
