package spk;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;


public class SimpleApp {
    public static void main(String[] args) {
        // Should be some file on your system
        String logFile = "file:///C:/datas/README.md";
        SparkConf conf = new SparkConf();
        conf.setMaster("local[2]").setAppName("Simple App");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile(logFile).cache();

        JavaRDD<String> shellRdd = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String line) {
                if (line.contains("shell")) {
                    System.out.println("========================");
                    System.out.println(line);
                }
                return line.contains("shell");
            }
        });

        System.out.println(shellRdd.count());
        getContainsCount(lines);
        sc.close();
    }

    /**
     * 包含a
     */
    private static void getContainsCount(JavaRDD<String> lines) {
        long numAs = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("##");
            }
        }).count();

        long numBs = lines.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("shell");
            }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with shell: " + numBs);
    }
}
