package com.etl;

import org.apache.spark.sql.*;

import java.util.*;

public interface FlatFileParse {

    public String run(SparkSession sp, String flatFilePath, String savePath);

    Dataset<Row> parseFlatFile(SparkSession sp, String inputPath);

    Dataset<Row> transformDS(Dataset<Row> ds);

    String saveData(Dataset ds, String savePath);

    HashMap getSchema();

}
