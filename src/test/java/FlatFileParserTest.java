import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import com.example.SerUtil;
import com.example.FlatFileParser;
import com.google.common.primitives.Ints;

public class FlatFileParserTest {
    private static Dataset<Row> df;
    private static SparkSession spark;

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession.builder().master("local[*]").config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(FlatFileParserTest.class.getName()).getOrCreate();
        df = spark.read().format("csv").option("header", "true").load("src/test/resources/credithistory.csv");
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testlsplit1() {
        int[] rawArr = {0, 1, 2, 3};
        List<Integer> schema = Ints.asList(rawArr);

        String testStr = "122333 garbage";

        Row result = SerUtil.lsplit(schema, testStr);

        Assert.assertEquals(null, result.getString(0));
        Assert.assertEquals("1", result.getString(1));
        Assert.assertEquals("22", result.getString(2));
        Assert.assertEquals("333", result.getString(3));
    }

    @Test
    public void testlsplit2() {
        int[] rawArr = {2, 1, 1, 1, 1, 1, 3};
        List<Integer> schema = Ints.asList(rawArr);

        String testStr = " ñüáéíóú ú";

        Row result = SerUtil.lsplit(schema, testStr);

        Assert.assertEquals("ñ", result.getString(0));
        Assert.assertEquals("ü", result.getString(1));
        Assert.assertEquals("á", result.getString(2));
        Assert.assertEquals("é", result.getString(3));
		Assert.assertEquals("í", result.getString(4));
        Assert.assertEquals("ó", result.getString(5));
		Assert.assertEquals("ú ú", result.getString(6));
    }

    @Test
    public void testParserBasic() {

        String flatFilePath = "src/test/resources/sourceFlatFileTest1.txt";
        String schemaFilePath = "src/test/resources/schematest1.csv";

        FlatFileParser parser = new FlatFileParser(spark, flatFilePath, schemaFilePath);
        Dataset<Row> result = parser.getDataset(spark);
        result.show();
        df.show();
        Assert.assertEquals(df.collectAsList(), result.collectAsList());
    }
}