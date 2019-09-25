import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.*;
import com.example.FlatFileGenerator;
import java.util.stream.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FlatFileGeneratorTest {
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
    public void testMakerBasic() {
        String outputPath = "src/test/resources/output/";
        String schemaPath = "src/test/resources/sample-schema-1.csv";
        String filePath = FlatFileGenerator.genFlatFile(
                spark,
                schemaPath,
                outputPath,
                df,
                false);

        List<String> result = readFileHelper(filePath.concat("part-00000"));
        List<String> x = readFileHelper("src/test/resources/sample-generated-1.txt");

        Assert.assertEquals(x, result);
    }

    private List<String> readFileHelper(String filePath) {
        List<String> list = null;
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
        list = lines.collect(Collectors.toList());
        } catch (IOException e) {
            System.out.println("Something is wrong.");
        }
        return list;
    }
}

