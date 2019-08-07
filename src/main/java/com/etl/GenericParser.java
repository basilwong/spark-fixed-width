package com.etl;

import org.apache.spark.sql.*;
import org.apache.spark.api.java.*;
import org.apache.spark.sql.types.*;

import java.util.*;
import java.util.stream.Collectors;
import java.io.Serializable;

public class GenericParser implements FlatFileParse, Serializable{
    
    public List<Integer> sizeOfColumn;
    public List<String> stringSchema;

    @Override
    public String run(SparkSession sp, String flatFilePath, String savePath) {
        Dataset ds = parseFlatFile(sp, flatFilePath);
        ds.show();
        ds = transformDS(ds);
        ds.show();
        return saveData(ds, savePath);
    }

    @Override
    public Dataset<Row> parseFlatFile(SparkSession sp, String flatFilePath) {

        List<StructField> headers = this.stringSchema
                .stream()
                .map(str -> new StructField(str, DataTypes.StringType, true, Metadata.empty()))
                .collect(Collectors.toList());

        StructType schemaStruct = DataTypes.createStructType(headers);
        JavaRDD<String> strRDD = sp.sparkContext().textFile(flatFilePath, 1).toJavaRDD();
        JavaRDD<Row> rowRDD = strRDD.map(str -> FlatFileUtil.lSplit(this.sizeOfColumn, str));

        return sp.createDataFrame(rowRDD, schemaStruct);
    }

    @Override
    public Dataset<Row> transformDS(Dataset<Row> ds) {
        if (TransUtil.hasColumn(ds, "ID")) {
            ds = ds.withColumn("ID", ds.col("ID").cast(DataTypes.IntegerType));
        }
        if (TransUtil.hasColumn(ds, "Name")) {
            System.out.println("has the column name");
           ds = ds.withColumn("Name", TransUtil.handleSpanish(ds.col("Name")));
        }

        return ds;
    }

    @Override
    public String saveData(Dataset ds, String savePath) {
        String outputPath = FlatFileUtil.determinePath(savePath);
        ds.write().parquet(outputPath);
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
