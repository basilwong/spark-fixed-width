package com.etl;

import org.apache.spark.sql.*;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import java.sql.Timestamp;

public class FlatFileUtil implements Serializable {

    public static Row lSplit(List<Integer> pos, String str) {

        List<String> cols = new ArrayList<>();
        int start = 0;
        for (Integer col_pos : pos) {
            if (str.length() <= col_pos + start) {
                if (str.length() <= start) {
                    cols.add(null);
                } else {
                    String val = str.substring(start).trim();
                    if (val.isEmpty()) {
                        cols.add(null);
                    } else {
                        cols.add(val);
                    }
                }
            } else if (str.length() > col_pos + start) {
                String val = str.substring(start, start + col_pos).trim();
                if (val.isEmpty()) {
                    cols.add(null);
                } else {
                    cols.add(val);
                }
            } else {
                cols.add(null);
            }
            start += col_pos;
        }
        return RowFactory.create(cols.toArray());
    }


    public static String rowToFWSTring(List<Integer> rowSize, Row r, Boolean leftpad) {

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

    public static List<Integer> getColSizes(SparkSession sc, String schemaFilePath) {
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

    public static String determinePath(String outputPath) {
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        return outputPath
                .concat(timestamp
                        .toString()
                        .replaceAll(":", "")
                        .replaceAll(" ", "-"))
                .concat("/");
    }

    public static Dataset<Row> readRawSchema(SparkSession sc, String schemaFilePath) {
        return sc
                .read()
                .format("csv")
                .option("header","True")
                .load(schemaFilePath);
    }

    public static List<Integer> readColSizes(Dataset<Row> rawRead) {
        return rawRead
                .select("size")
                .rdd()
                .toJavaRDD()
                .map(row -> Integer.valueOf((String) row.get(0)))
                .collect();
    }

    public static List<String> readSchema(Dataset<Row> rawRead) {
        return rawRead
                .select("col_name")
                .rdd()
                .toJavaRDD()
                .map(row -> ((String) row.get(0)).trim())
                .collect();
    }
}
