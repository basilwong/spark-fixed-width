package com.example;

import org.apache.spark.api.java.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class TransUtil implements Serializable{

    public static Column setColLength(Column c, int n) {
        return functions.substring(functions.lpad(c, n, " "), 0, n);
    }

    public static Column toFlatFileDate(Column c) {
        return functions.date_format(functions.to_timestamp(c, "MM/dd/yyyy"), "yyyyMMdd");
    }

}
