package demo.io.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.Base64;
import java.util.Iterator;

/**
 * use java and spark 2 connect HBase
 *
 * @author admin 20-05-19
 */
public class DemoHbase implements Serializable {
    private static final long serialVersionUID = -6266105108442536448L;

    static Configuration cfg;
    static Connection conn;
    static int size = 3;

    public static void main(String[] args) throws IOException {
        initHbase();
        String rowKey = "rowKey";
        TableName tableName = TableName.valueOf("stu");
        System.out.println(tableName.getNameAsString().equals(tableName.toString()));
        Admin admin = conn.getAdmin();
        Table table = admin.tableExists(tableName) ? conn.getTable(tableName) : null;
        if (null != table) {
            getRowByRowKey(table, rowKey);
            getScanRows(table, size);

            //hbase2Rdd(tableName, size);

            table.close();
        }

        conn.close();

    }

    /* spark */
    private static void hbase2Rdd(TableName tableName, int size) throws IOException {
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = getHbaseRdd(tableName, size);
        hbaseRdd.map((Function<Tuple2<ImmutableBytesWritable, Result>, Result>) v1 -> v1._2).map((Function<Result, String>) result -> {
            String row = "";
            System.out.println(Bytes.toString(result.getRow()));
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                row = row.concat(",").concat(value);
            }
            return row.substring(1);
        }).foreach((VoidFunction<String>) row -> System.out.println(row));
    }

    private static JavaPairRDD<ImmutableBytesWritable, Result> getHbaseRdd(TableName tableName, int size) throws IOException {
        Scan scan = new Scan().setCaching(1).setFilter(new PageFilter(size));
        cfg.set(TableInputFormat.INPUT_TABLE, tableName.toString());
        ClientProtos.Scan toScan = ProtobufUtil.toScan(scan);
        cfg.set(TableInputFormat.SCAN, new String(Base64.getEncoder().encode(toScan.toByteArray())));
        SparkConf conf = new SparkConf();
        conf.setMaster("local").setAppName("DemoHbase");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaPairRDD<ImmutableBytesWritable, Result> hbaseRdd = jsc.newAPIHadoopRDD(cfg, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        hbaseRdd.foreach(new VoidFunction<Tuple2<ImmutableBytesWritable, Result>>() {
            private static final long serialVersionUID = 1L;

            @Override
            public void call(Tuple2<ImmutableBytesWritable, Result> tp) {
                System.out.println("rowKey: " + Bytes.toString(tp._1.get()));
                System.out.println(tp._2);
            }
        });
        return hbaseRdd;
    }

    private static void getScanRows(Table table, int size) throws IOException {
        String rowKey;
        Scan scan = new Scan().setCaching(1).setFilter(new PageFilter(size));
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()) {
            Result next = iterator.next();
            rowKey = new String(next.getRow());
            getRowByRowKey(table, rowKey);
        }
    }

    private static void getRowByRowKey(Table table, String rowKey) throws IOException {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = table.get(get);
        foreachResultCells(result);
    }

    private static void foreachResultCells(Result result) {
        System.out.println(Bytes.toString(result.getRow()));
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            String family = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getQualifierLength());
            String col = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            System.out.println(family + "." + col + "\t= " + value);
        }
    }

    private static void initHbase() throws IOException {
        cfg = HBaseConfiguration.create();
        /* you can find the key in HBaseConfiguration */
        cfg.set("hbase.zookeeper.quorum", "master");
        cfg.set("hbase.zookeeper.property.clientPort", "2181");
        conn = ConnectionFactory.createConnection(cfg);
    }
}
