package spk.core;

import demo.utils.spk.MySparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * aggregateByKey(zeroValue: U, seqFunc: JFunction2[U, V, U], combFunc: JFunction2[U, U, U])
 * zeroValue: 初始化V的返回值
 * seqFunc：分别在各自分区内聚合key相同的V
 * combFunc：将所有分区的值聚合到一起
 * 该函数主要是对key对应的Value进行操作；不同的分区和不同的操作可能影响结果集
 *
 * @author admin 2020-7-13
 */
public class DmPairRDD {
    private static JavaSparkContext jsc = MySparkUtil.jsc("DmPairRDD");

    public static void main(String[] args) {

        JavaPairRDD<String, ArrayList<Integer>> aggregate = getStringArrayListJavaPairRDD();
//        System.out.println(String.format("v1 %s v2 %s", v1, v2));
//        System.out.println(String.format("u1 %s u2 %s", v1, v2));
        System.out.println("================");
        System.out.println(aggregate.take(3));
//        System.out.println(aggregate.count());

    }

    public static JavaPairRDD<String, ArrayList<Integer>> getStringArrayListJavaPairRDD() {
        List<Object[]> dataList = getDataList();
        JavaRDD<Object[]> parallelize = jsc.parallelize(dataList, 3); // dataList.size() % 10
        JavaPairRDD<String, Integer> pairRDD = parallelize.mapToPair(new PairFunction<Object[], String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Object[] objects) throws Exception {
                return new Tuple2<>(objects[0].toString(), Integer.valueOf(objects[1].toString()));
            }
        });
        pairRDD.foreach((VoidFunction<Tuple2<String, Integer>>) objects -> System.out.println(objects));
        System.out.println("================");
        System.out.println(pairRDD.partitions().size());
//        JavaPairRDD<String, Tuple2<String, List<Integer>>> aggregate = aggregate1(pairRDD);
        return pairRDD.aggregateByKey(new ArrayList<Integer>(), new Function2<ArrayList<Integer>, Integer, ArrayList<Integer>>() {
            @Override
            public ArrayList<Integer> call(ArrayList<Integer> v1, Integer v2) throws Exception {
                System.out.println(String.format("v1 %s v2 %s", v1, v2));
                v1.add(v2);
                return v1;
            }
        }, new Function2<ArrayList<Integer>, ArrayList<Integer>, ArrayList<Integer>>() {
            @Override
            public ArrayList<Integer> call(ArrayList<Integer> v1, ArrayList<Integer> v2) throws Exception {
                System.out.println(String.format("u1 %s u2 %s", v1, v2));
                v1.addAll(v2);
                return v1;
            }
        });
    }

    private static JavaPairRDD<String, Tuple2<String, List<Integer>>> aggregate1(JavaPairRDD<String, Integer> pairRDD) {
        return pairRDD.aggregateByKey(new Tuple2<String, List<Integer>>("", new ArrayList<>()), new Function2<Tuple2<String, List<Integer>>, Integer, Tuple2<String, List<Integer>>>() {
            @Override
            public Tuple2<String, List<Integer>> call(Tuple2<String, List<Integer>> v1, Integer v2) throws Exception {
                System.out.println(String.format("v1 %s v2 %s", v1, v2));
                v1._2.add(v2);
                return v1;
            }
        }, new Function2<Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>, Tuple2<String, List<Integer>>>() {
            @Override
            public Tuple2<String, List<Integer>> call(Tuple2<String, List<Integer>> v1, Tuple2<String, List<Integer>> v2) throws Exception {
                System.out.println(String.format("u1 %s u2 %s", v1, v2));
                if (v1._1.equals(v2._1)) {
                    v1._2.add(v2._2.get(0));
                }
                System.out.println(v1);
                return v1;
            }
        });
    }

    private static List<Object[]> getDataList() {
        List<Object[]> list = new ArrayList<>();
        list.add(new Object[]{"a", 1});
        list.add(new Object[]{"a", 2});
        list.add(new Object[]{"a", 3});
        list.add(new Object[]{"b", 4});
        list.add(new Object[]{"b", 5});
        list.add(new Object[]{"c", 6});
        return list;
    }
}
