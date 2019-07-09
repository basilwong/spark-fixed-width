package com.example;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import java.util.*;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

public class FlatFileMaker {

    public static void genFlatFile(String outputPath, Dataset<Row> data, List<Integer> colSizes) {
        JavaRDD<String> trip = data.rdd().toJavaRDD().map(row-> rowToFWSTring(colSizes, row));
        trip.saveAsTextFile(determinePath(outputPath));
    }

    private static String rowToFWSTring(List<Integer> rowSize, Row r) {
        String[] vals = new String[r.size()];
        for (int i = 0; i < r.size(); i++) {
            vals[i] = StringUtils.leftPad(r.getString(i), rowSize.get(i), ' ').substring(0, rowSize.get(i));
        }
        return String.join("", vals);
    }

    private static String determinePath(String outputPath) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return outputPath + timestamp.toString().replaceAll(":", "") + "/";
    }
}
