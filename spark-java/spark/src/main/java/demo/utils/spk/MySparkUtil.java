package demo.utils.spk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author admin 2020-7-13
 */
public class MySparkUtil {

    public static JavaSparkContext jsc(String master, String appName) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master);
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        return jsc;
    }

    public static JavaSparkContext jsc(String appName) {
        SparkConf conf = new SparkConf().setAppName(appName).setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.setLogLevel("WARN");
        return jsc;
    }

}
