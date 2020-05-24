package demo.io.files;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.List;

public class TextFile {
    static final String inputPath = "file:///C:/datas/README.md";
    static final String outputPath = "file:///C:/datas/out";

    public static void main(String[] args) {
        // Should be some file on your system
        JavaSparkContext sc = new JavaSparkContext("local", "TextFile");
        JavaRDD<String> logData = sc.textFile(inputPath);

        JavaRDD<String> result = filterLine(logData, sc);

        saveByRdd(result);
    }

    /**
     * 每次执行前，删除旧的 out || 每次执行后删除
     *
     * @OutPath C:/datas/out
     * @Exception java.io.IOException: (null) entry in command string: null chmod 0644
     * @Solve 下载hadoop.dll文件并拷贝到c:\windows\system32目录中
     */
    private static void saveByRdd(JavaRDD<String> result) {
        // TODO check out and delete it
        result.saveAsTextFile(outputPath);
    }

    private static JavaRDD<String> filterLine(JavaRDD<String> logData, JavaSparkContext sc) {
        long numAs = logData.filter((s) -> s.contains("a")).count();
        long numBs = logData.filter((Function<String, Boolean>) s -> s.contains("b")).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        List<String> list = new ArrayList<>();
        list.add("Lines with a: " + numAs);
        list.add("lines with b: " + numBs);
        System.out.println("list.size: " + list.size());

        return sc.parallelize(list);
    }
}
