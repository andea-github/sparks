package spk.mllib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import scala.collection.immutable.Map;

import java.util.Arrays;
import java.util.List;

/**
 * 关联算法原理：
 * - 根据 support 选择主体(事件A发生的概率)范围
 * - 然后根据 confidence 筛选和主体关联事件(事件B伴随A发生的概率)
 * <p>
 * 1)支持度 support(A => B) = P(AnB) = |A n B| / |N|，表示数据集D中，事件A和事件B共同出现的概率；
 * <p>
 * 2)置信度 confidence(A => B) = P(B|A) = |A n B| / |A|，表示数据集D中，出现事件A的事件中出现事件B的概率；
 * <p>
 * 3）提升度 lift(A => B) = P(B|A):P(B) = |A n B| / |A| : |B| / |N|，表示数据集D中，出现A的条件下出现事件B的概率和没有条件A出现B的概率；
 */
public class DemoFP {
    static String data_path = "file:///C:/datas/test/";       //数据集路径
    static double minSupport = 0.5;     //最小支持度
    static double minConfidence = 0.0;  //最小置信度
    static int numPartition = 10;       //数据分区

    public static void main(String[] args) {
        String input = data_path.concat("growth_chars.txt");
        initArgs(args);
        JavaRDD<List<String>> transactions = getDataSet(input);

        //创建FPGrowth的算法实例，同时设置好训练时的最小支持度和数据分区
        FPGrowth fpGrowth = new FPGrowth().setMinSupport(0).setNumPartitions(numPartition);
        FPGrowthModel<String> model = fpGrowth.run(transactions);//执行算法

        /**
         * 查看所有频繁諅，并列出它出现的次数 (每个数据集 元素唯一)
         * [[a]],3
         * [[b]],2
         * [[c]],1
         * [[b, a]],2
         * [[c, b]],1
         * [[c, a]],1
         * [[c, b, a]],1
         * support(A => B) = P(AnB) = |A n B| / |N|，表示数据集D中，事件A和事件B共同出现的概率；
         * P(D) = lines.count() = 3
         * P(a) =  3    => support(a) = 3/3
         * P(b) =  2    => support(b) = 2/3
         * P(c) =  1    => support(c) = 1/3
         * Map(a -> 1.0, b -> 0.6666666666666666, c -> 0.3333333333333333)
         */
        for (FPGrowth.FreqItemset<String> itemset : model.freqItemsets().toJavaRDD().collect())
            System.out.println("=== [" + itemset.javaItems() + "]," + itemset.freq());
        Map<String, Object> itemSupport = model.itemSupport();
        System.out.println(itemSupport);

        /**
         * confidence(A => B) = P(B|A) = |A n B| / |A|，表示数据集D中，出现事件A的事件中出现事件B的概率
         * P(a) =  3    a 出现3次
         * [[b, a]],2   b 伴随 a 出现 2次
         * P(AnB) = 2   => confidence(a => b) = 2/3     [a] => [b], 0.6666666666666666
         * [[c, a]],1   c 伴随 a 出现 1次
         * P(AnC) = 1   => confidence(a => c) = 1/3     [a] => [c], 0.3333333333333333
         */
        generateRules(model);
    }

    /**
     * 通过置信度筛选出强规则
     * antecedent 表示前项
     * consequent 表示后项
     * confidence 表示规则的置信度 0.0 可以查看所有 >= 0.0 的置信度
     */
    private static void generateRules(FPGrowthModel<String> model) {
        for (AssociationRules.Rule<String> rule : model.generateAssociationRules(minConfidence).toJavaRDD().collect())
            System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
    }

    private static JavaRDD<List<String>> getDataSet(String input) {
        SparkConf conf = new SparkConf().setAppName("DemoFP").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //加载数据，并将数据通过空格分割
        return sc.textFile(input)
                .map((Function<String, List<String>>) s -> {
                    String[] parts = s.split(" ");
                    return Arrays.asList(parts);
                });
    }


    private static void initArgs(String[] args) {
        if (args.length < 0) {
            System.out.println("<input data_path>");
            System.exit(-1);
        }
//        data_path = args[0];
        if (args.length >= 2)
            minSupport = Double.parseDouble(args[1]);
        if (args.length >= 3)
            numPartition = Integer.parseInt(args[2]);
        if (args.length >= 4)
            minConfidence = Double.parseDouble(args[3]);
    }
}
