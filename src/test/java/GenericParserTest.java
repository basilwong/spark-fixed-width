import com.etl.GenericParser;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

public class GenericParserTest {
    private static Dataset<Row> df;
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession
                .builder()
                .master("local[*]")
                .config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(GenericParserTest.class.getName())
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
    public void testParseFlatFile1() {
        String schemaPath = "src/test/resources/schema1.csv";
        String flatFilePath = "src/test/resources/fw1.txt";
        String outptuPath = "src/test/resources/output/";
        GenericParser gp = new GenericParser();
        gp.updateSchema(spark, schemaPath);
        String x = gp.run(spark, flatFilePath, outptuPath);

        Dataset<Row> expected = spark.read().parquet("src/test/resources/parquet1.parquet");

        File[] fileArr = TestUtil.searchFiles(x, "part-", ".parquet");
        System.out.print(x);
        Dataset<Row> result = spark.read().parquet(fileArr[0].getPath());

        Assert.assertEquals(expected.collectAsList(), result.collectAsList());
    }

    @Test
    public void testParseFlatFile2() {
        String schemaPath = "src/test/resources/schema2.csv";
        String flatFilePath = "src/test/resources/fw2.txt";
        String outptuPath = "src/test/resources/output/";
        GenericParser gp = new GenericParser();
        gp.updateSchema(spark, schemaPath);
        String x = gp.run(spark, flatFilePath, outptuPath);

        Dataset<Row> expected = spark.read().parquet("src/test/resources/parquet2.parquet");

        File[] fileArr = TestUtil.searchFiles(x, "part-", ".parquet");
        System.out.print(x);
        Dataset<Row> result = spark.read().parquet(fileArr[0].getPath());

        Assert.assertEquals(expected.collectAsList(), result.collectAsList());
    }

}

