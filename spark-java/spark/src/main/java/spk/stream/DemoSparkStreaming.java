package spk.stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * @author admin 2020-5-20
 */
public class DemoSparkStreaming {
    static SparkConf conf;
    static JavaSparkContext jsc;
    static JavaStreamingContext jssc;
    static String inputPath = "src/test/ssc";

    public static void main(String[] args) {
        conf = new SparkConf().setMaster("local[2]").setAppName("DemoSparkStreaming");
//        conf.set("spark.driver.allowMultipleContexts", "true");
        jsc = new JavaSparkContext(conf);
        jssc = new JavaStreamingContext(jsc, Durations.seconds(15));
        //testPathByRdd();
        getDstream();

    }

    private static void getDstream() {
        JavaDStream<String> dStream = jssc.textFileStream(inputPath);
        dStream.foreachRDD(new VoidFunction2<JavaRDD<String>, Time>() {
            @Override
            public void call(JavaRDD<String> v1, Time v2) throws Exception {
                if (v1.count() > 0) {
                    System.out.println("1)=========== " + v1.first());
                }

            }
        });
        /*JavaDStream<String> words = dStream.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairDStream<String, Integer> wdCounts = words.mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);
        wdCounts.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) pairRDD -> {
            pairRDD.saveAsTextFile("src/test/ssc-out");
            pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) pair -> System.out.println("============" + pair));
        });*/
        dStream.print(); // ~ take(10)
//        JavaPairRDD<String, Integer> compute = wdCounts.compute(new Time(1000));
//        compute.foreach((VoidFunction<Tuple2<String, Integer>>) tp -> System.out.println(tp));
//        compute.saveAsTextFile("src/test/ssc-out");
//        wdCounts.saveAsT
        jssc.start();
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void testPathByRdd() {
        JavaRDD<String> stringJavaRDD = jsc.textFile(inputPath.concat("/log1.txt"));
        System.out.println("============ " + stringJavaRDD.first());
    }

}
