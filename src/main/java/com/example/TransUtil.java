package com.example;

import org.apache.spark.sql.*;
import java.io.Serializable;

public class TransUtil implements Serializable{

    public static Column setColLength(Column c, int n) {
        return functions.substring(functions.lpad(c, n, " "), 0, n);
    }

    // TODO: Add modularity to the formatting of the dates
    public static Column toFlatFileDate(Column c, String inFormat, String outFormat) {
        return functions.date_format(functions.to_timestamp(c, inFormat), outFormat);
    }

    public static Column handleSpanish(Column c) {
        String testStr = " ñüáéíóú ú";
        return functions.translate(c, "ñüáéíóú", "nuaeiou");
    }

}
