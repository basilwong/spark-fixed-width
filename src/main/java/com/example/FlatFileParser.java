package com.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

class SerUtil implements Serializable {

    public static Row lsplit(List<Integer> pos, String str) {

        List<String> cols = new ArrayList<>();
        int start = 0;
        for (Integer col_pos : pos) {
            if (str.length() <= col_pos + start) {
                if (str.length() <= start) {
                    cols.add("");
                } else {
                    cols.add(str.substring(start).trim());
                }
            } else if (str.length() > col_pos + start) {
                cols.add(str.substring(start, start + col_pos).trim());
            } else {
                cols.add("");
            }
            start += col_pos;
        }
        return RowFactory.create(cols.toArray());
    }
}


public class FlatFileParser implements Serializable {

    private String flatFilePath;
    private String schemaFilePath;
    private List<Integer> sizeOfColumn;
    private List<String> stringSchema;

     FlatFileParser(SparkSession sc, String flatFilePath, String schemaFilePath) {
         this.flatFilePath = flatFilePath;
         this.schemaFilePath = schemaFilePath;
         Dataset<Row> readraw = readSchema(sc);
         readColSizes(readraw);
         readSchema(readraw);
    }

    private Dataset<Row> readSchema(SparkSession sc) {
        return sc
                .read()
                .format("csv")
                .option("header","True")
                .load(this.schemaFilePath);
    }

    private void readColSizes(Dataset<Row> rawRead) {
        this.sizeOfColumn = rawRead
                .select("size")
                .rdd()
                .toJavaRDD()
                .map(row -> Integer.valueOf((String) row.get(0)))
                .collect();
    }

    private void readSchema(Dataset<Row> rawRead) {

        this.stringSchema = rawRead
                .select("col_name")
                .rdd()
                .toJavaRDD()
                .map(row -> ((String) row.get(0)).trim())
                .collect();

    }

    public Dataset<Row> getDataset(SparkSession sc) {

        List<StructField> headers = this.stringSchema
                .stream()
                .map(str -> new StructField(str, DataTypes.StringType, true, Metadata.empty()))
                .collect(Collectors.toList());

        StructType schemaStruct = DataTypes.createStructType(headers);
        JavaRDD<String> strRDD = sc.sparkContext().textFile(this.flatFilePath, 1).toJavaRDD();
        JavaRDD<Row> rowRDD = strRDD.map(str -> SerUtil.lsplit(this.sizeOfColumn, str));

        return sc.createDataFrame(rowRDD, schemaStruct);
    }

    public List<Integer> getColSizes(){
        return this.sizeOfColumn;
    }

    public List<String> getStringSchema(){
        return this.stringSchema;
    }


    public static Dataset<Row> parseFlatFile(SparkSession sc, String flatFilePath, String schemaFilePath) {

        Dataset<Row> rawRead = sc.read()
                .format("csv")
                .option("header","True")
                .load(schemaFilePath);

        List<Integer> sizeOfColumn = rawRead
                .select("size")
                .rdd()
                .toJavaRDD()
                .map(row -> Integer.valueOf((String) row.get(0)))
                .collect();

        List<StructField> headers = rawRead
                .select("col_name")
                .rdd()
                .toJavaRDD()
                .map(row -> new StructField(((String) row.get(0)).trim(), DataTypes.StringType, true, Metadata.empty()))
                .collect();

        StructType schema1 = DataTypes.createStructType(headers);

        JavaRDD<String> strRDD = sc.sparkContext().textFile(flatFilePath, 1).toJavaRDD();
        JavaRDD<Row> rowRDD = strRDD.map(str -> SerUtil.lsplit(sizeOfColumn, str));

        return sc.createDataFrame(rowRDD, schema1);

    }

}
