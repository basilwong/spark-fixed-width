package com.example;

import org.apache.spark.sql.*;
import java.io.Serializable;

public class TransUtil implements Serializable{

    public static Column setColLength(Column c, int n) {
        return functions.substring(functions.lpad(c, n, " "), 0, n);
    }

    public static Column toFlatFileDate(Column c) {
        return functions.date_format(functions.to_timestamp(c, "MM/dd/yyyy"), "yyyyMMdd");
    }

    public static Column handleSpanish(Column c) {
        String testStr = " ñüáéíóú ú";
        return functions.translate(c, "ñüáéíóú", "nuaeiou");
    }

}
