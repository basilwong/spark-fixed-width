import java.io.Serializable;
import com.example.FlatFileParser;
import com.example.SerUtil;

import org.apache.spark.sql.*;
import java.util.*;
import com.google.common.primitives.Ints;
import org.apache.spark.api.java.JavaPairRDD;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;



public class FlatFileParserTest implements Serializable{

    private static final long serialVersionUID = 1L;
    private transient SparkSession sp;

    @Before
    public void setUp() {
        SparkSession sp = SparkSession.builder().master("local").appName("test").getOrCreate();
    }

    @After
    public void tearDown() {
        if (sp != null){
            sp.stop();
        }
    }

    @Test
    public void testlsplit1() {
        int[] rawArr = {0, 1, 2, 3};
        List<Integer> schema = Ints.asList(rawArr);

        String testStr = "122333 garbage";

        Row result = SerUtil.lsplit(schema, testStr);

        Assert.assertEquals("", result.getString(0));
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

}