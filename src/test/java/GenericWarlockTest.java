import com.etl.GenericWarlock;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.io.File;

public class GenericWarlockTest {
    private static Dataset<Row> df;
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession
                .builder()
                .master("local[*]")
                .config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(GenericWarlockTest.class.getName())
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        df = spark
                .read()
                .format("csv")
                .option("header", "true")
                .load("src/test/resources/data3.csv");
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testGenFlatFile1() {
        String inputPath = "src/test/resources/parquet1.parquet";
        String outputPath = "src/test/resources/output/";
        String schemaPath = "src/test/resources/schema1.csv";
        GenericWarlock gw = new GenericWarlock();
        gw.updateSchema(spark, schemaPath);
        String filePath = gw.run(spark, inputPath, outputPath);

        List<String> expected = TestUtil.readFileHelper("src/test/resources/generated1.txt");

        File[] fileArr = TestUtil.searchFiles(filePath, "part-", "");
        List<String> result = TestUtil.readFileHelper(fileArr[0].getPath());

        Assert.assertEquals(expected, result);
    }

    @Test
    public void testGenFlatFile2() {
        String inputPath = "src/test/resources/parquet2.parquet";
        String outputPath = "src/test/resources/output/";
        String schemaPath = "src/test/resources/schema2.csv";
        GenericWarlock gw = new GenericWarlock();
        gw.updateSchema(spark, schemaPath);
        String filePath = gw.run(spark, inputPath, outputPath);

        List<String> expected = TestUtil.readFileHelper("src/test/resources/generated2.txt");

        File[] fileArr = TestUtil.searchFiles(filePath, "part-", "");
        List<String> result = TestUtil.readFileHelper(fileArr[0].getPath());

        Assert.assertEquals(expected, result);
    }
}

