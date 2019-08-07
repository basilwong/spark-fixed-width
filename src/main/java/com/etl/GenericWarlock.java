package com.etl;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;

import java.util.*;
import java.io.Serializable;

public class GenericWarlock implements FlatFileWarlock, Serializable{

    public List<Integer> sizeOfColumn;
    public List<String> stringSchema;

    @Override
    public String run(SparkSession sp, String inputPath, String flatFilePath) {
        Dataset<Row> ds = readInput(sp, inputPath);
        ds.show();
        ds = transformDS(ds);
        JavaRDD<String> jr = prepData(sp, ds);
        return createFlatFile(jr, flatFilePath);
    }

    @Override
    public Dataset<Row> readInput(SparkSession sp, String inputPath) {
        return sp.read().parquet(inputPath);
    }

    @Override
    public Dataset<Row> transformDS(Dataset<Row> ds) {
        return ds;
    }

    @Override
    public JavaRDD<String> prepData(SparkSession sp, Dataset<Row> ds) {
        return ds.rdd()
                .toJavaRDD()
                .map(row-> FlatFileUtil.rowToFWSTring(this.sizeOfColumn, row, true));
    }

    @Override
    public String createFlatFile(JavaRDD<String> jr, String flatFilePath) {
        String outputPath = FlatFileUtil.determinePath(flatFilePath);
        jr.saveAsTextFile(outputPath);
        return outputPath;
    }

    public void updateSchema(SparkSession sp, String schemaFilePath) {
        Dataset<Row> readraw = FlatFileUtil.readRawSchema(sp, schemaFilePath);
        this.sizeOfColumn = FlatFileUtil.readColSizes(readraw);
        this.stringSchema = FlatFileUtil.readSchema(readraw);
    }

    @Override
    public HashMap<String, Integer> getSchema() {
        HashMap<String, Integer> schema = new HashMap<String, Integer>();
        for (int i = 0; i < this.sizeOfColumn.size(); i++) {
            schema.put(this.stringSchema.get(i), this.sizeOfColumn.get(i));
        }
        return schema;
    }
}

