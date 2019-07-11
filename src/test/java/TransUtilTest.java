import com.example.TransUtil;

import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;

import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Assert;

import java.lang.reflect.Array;
import java.util.*;

public class TransUtilTest {
    private static Dataset<Row> df;
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession
                .builder()
                .master("local[*]")
                .config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(FlatFileParserTest.class.getName())
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        df = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("src/test/resources/credit-history.csv");
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testSetCol1() {

        String[] expectedRaw = {"aaaaa", "aaaaa", "     ", "    1"};
        List<String> expectedList = Arrays.asList(expectedRaw);

        Dataset expected = spark.createDataset(expectedList, Encoders.STRING()).toDF();

        String[] testRaw = {"aaaaa", "aaaaaa", "", "1"};
        List<String> testList = Arrays.asList(testRaw);

        Dataset testDS = spark.createDataset(testList, Encoders.STRING()).toDF();

        for (String c : testDS.columns()) {
            testDS = testDS.withColumn(c, TransUtil.setColLength(testDS.col(c), 5));
        }

        Assert.assertEquals(expected.collectAsList(), testDS.collectAsList());
    }

    @Test
    public void testDateFormatPos() {

        String[] expectedRaw = {"20000101", "20200229", "19991231", "11110301"};
        List<String> expectedList = Arrays.asList(expectedRaw);

        Dataset expected = spark.createDataset(expectedList, Encoders.STRING()).toDF();

        String[] testRaw = {"01/01/2000", "02/29/2020", "12/31/1999", "3/1/1111"};
        List<String> testList = Arrays.asList(testRaw);

        Dataset testDS = spark.createDataset(testList, Encoders.STRING()).toDF();

        for (String c : testDS.columns()) {
            testDS = testDS.withColumn(c, TransUtil.toFlatFileDate(testDS.col(c), "MM/dd/yyyy", "yyyyMMdd"));
        }

        Assert.assertEquals(expected.collectAsList(), testDS.collectAsList());
    }

    @Test
    public void testDateFormatNeg() {

        String[] expectedRaw = {null, null, null, null};
        List<String> expectedList = Arrays.asList(expectedRaw);

        Dataset expected = spark.createDataset(expectedList, Encoders.STRING()).toDF();

        String[] testRaw = {"01.01.2000", "29/02/2020", "02/29/2019", "3/99"};
        List<String> testList = Arrays.asList(testRaw);

        Dataset testDS = spark.createDataset(testList, Encoders.STRING()).toDF();

        for (String c : testDS.columns()) {
            testDS = testDS.withColumn(c, TransUtil.toFlatFileDate(testDS.col(c), "MM/dd/yyyy", "yyyyMMdd"));
        }

        Assert.assertEquals(expected.collectAsList(), testDS.collectAsList());
    }

    @Test
    public void testHandleSpanish() {

        String[] expectedRaw = {"nuaeiou"};
        List<String> expectedList = Arrays.asList(expectedRaw);

        Dataset expected = spark.createDataset(expectedList, Encoders.STRING()).toDF();

        String[] testRaw = {"ñüáéíóú"};
        List<String> testList = Arrays.asList(testRaw);

        Dataset testDS = spark.createDataset(testList, Encoders.STRING()).toDF();

        for (String c : testDS.columns()) {
            testDS = testDS.withColumn(c, TransUtil.handleSpanish(testDS.col(c)));
        }

        Assert.assertEquals(expected.collectAsList(), testDS.collectAsList());
    }
}

