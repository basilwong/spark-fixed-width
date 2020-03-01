package com.etl;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import java.util.*;
import java.sql.Timestamp;

public class FlatFileGenerator {

    public static String genFlatFile(
            SparkSession sc,
            String schemaPath,
            String outputPath,
            Dataset<Row> data,
            Boolean leftpad) {

        List<Integer> colSizes = getColSizes(sc, schemaPath);
        JavaRDD<String> trip = data.rdd()
                .toJavaRDD()
                .map(row-> rowToFWSTring(colSizes, row, leftpad));

        String filePath = determinePath(outputPath);
        trip.saveAsTextFile(filePath);

        return filePath;
    }

    private static String rowToFWSTring(List<Integer> rowSize, Row r, Boolean leftpad) {

        String sign = "-";
        if (leftpad) { sign = ""; }

        String[] vals = new String[r.size()];

        for (int i = 0; i < r.size(); i++) {
            if (r.get(i) == null) {
                vals[i] = StringUtils.repeat(" ", rowSize.get(i));
            } else {
                vals[i] = String
                        .format("%" + sign + rowSize.get(i) + "s", r.get(i).toString())
                        .substring(0, rowSize.get(i));
            }
        }
        return String.join("", vals);
    }

    private static String determinePath(String outputPath) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return outputPath
                .concat(timestamp
                        .toString()
                        .replaceAll(":", "")
                        .replaceAll(" ", "-"))
                .concat("/");
    }

    private static List<Integer> getColSizes(SparkSession sc, String schemaFilePath) {
        Dataset<Row> rawRead = sc
                .read()
                .format("csv")
                .option("header","True")
                .load(schemaFilePath);
        return rawRead
                .select("size")
                .rdd()
                .toJavaRDD()
                .map(row -> Integer.valueOf((String) row.get(0)))
                .collect();
    }
}
