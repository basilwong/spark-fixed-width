package temp;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class testRead {
    private static Dataset<Row> df;
    private static SparkSession spark;
    private static final Logger logger = LoggerFactory.getLogger(testRead.class);

    @BeforeClass
    public static void beforeClass() {
        spark = SparkSession.builder().master("local[*]").config(new SparkConf().set("fs.defaultFS", "file:///"))
                .appName(testRead.class.getName()).getOrCreate();
        df = spark.read().csv("src/test/resources/credithistory.csv");
        logger.info("Created spark context and dataset with {} rows.", df.count());
    }

    @Test
    public void buildClippingTransformerTest() {
        logger.info("Testing the spark sorting function.");
        Dataset<Row> sorted = df.sort("_c7");
        Assert.assertEquals(sorted.count(), df.count());
    }

    @AfterClass
    public static void afterClass() {
        if (spark != null) {
            spark.stop();
        }
    }
}
