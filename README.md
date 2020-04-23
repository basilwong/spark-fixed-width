# Spark Fixed Width 

Framework for parsing fixed width column files into Spark Datasets and generating fixed width column files from Spark Datasets in Java. 

## Dependencies

Required dependencies added to the pom. Note that these classes will work on any Spark Version containing Datasets < 2.0.

```
<dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.11</artifactId>
        <version>2.3.2</version>
    </dependency>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.11</artifactId>
        <version>2.3.2</version>
    </dependency>
</dependencies>
```

## Utilization

FlatFileGenerator is used to generate flat files from a Dataset. FlatFileParser is used to parse a flat file into a Dataset. 

Examples of how to use the classes are in the main.java file. 

