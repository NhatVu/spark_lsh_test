package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkConfig {

    protected static SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("abc");
    protected static JavaSparkContext sc = new JavaSparkContext(conf);

}
