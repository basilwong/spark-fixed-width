package com.example;

import org.apache.spark.sql.*;
import java.io.Serializable;
import java.util.*;

public class SerUtil implements Serializable {

    public static Row lsplit(List<Integer> pos, String str) {

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
}
