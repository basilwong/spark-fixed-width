package com.etl;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.io.Serializable;
import java.util.Arrays;


public class TransUtil implements Serializable{

    /**
     * Function for setting the width of a StringType Column. Takes a column and the length as an
     * integer then performs substring and left pad.
     * @param c column
     * @param n length of column
     * @return column with the width fixed at a set length
     */
    public static Column setColLength(Column c, int n) {
        return functions.substring(functions.lpad(c, n, " "), 0, n);
    }

    /**
     * Edits the format of the dates in a column.
     * @param c column to be edited
     * @param inFormat specifies the format of the input
     * @param outFormat specifies the format of the output
     * @return column with edited dates
     */
    public static Column toFlatFileDate(Column c, String inFormat, String outFormat) {
        return functions.date_format(functions.to_timestamp(c, inFormat), outFormat);
    }

    /**
     * Translates certain letters with accents to normal letters.
     * @param c column with strings
     * @return column with strings with Spanish letters translated
     */
    public static Column handleSpanish(Column c) {
        return functions.translate(c, "ñüáéíóú", "nuaeiou");
    }

    /**
     * Checks if a Dataset has a column
     * @param ds Dataset
     * @param columnName name of the column to check for
     * @return true if the Dataset contains the column, false otherwise
     */
    public static boolean hasColumn(Dataset<Row> ds, String columnName) {
        return Arrays.asList(ds.columns()).contains(columnName);
    }

}
