package es;

import lombok.Data;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * @author admin 2020-6-16
 */
@Data
public class EsClient {
    private String clusterName = "elasticsearch"; //集群名称
    private String host = "localhost"; // 服务器ip地址
    private int port = 9300; // 端口
    private String index;   // 索引
    private Settings settings;
    private TransportAddress transportAddress;
    private TransportClient transportClient = null; // 6.x-
    private RestHighLevelClient restHighLevelClient; // 7.x+
    private SearchRequestBuilder requestBuilder;

    public EsClient() {
        initTransportClient();
    }

    public EsClient(String index) {
        this.index = index;
        initTransportClient();
    }

    public EsClient(String host, int port) {
        this.host = host;
        this.port = port;
//        initTransportClient();
        initHighLevelClient();
    }

    public EsClient(String clusterName, String host, int port) {
        this.clusterName = clusterName;
        this.host = host;
        this.port = port;
        initTransportClient();
        initHighLevelClient();
    }

    private void initTransportClient() {
        this.settings = Settings.builder()
                .put("cluster.name", this.clusterName)
                .put("client.transport.sniff", true) // 不用手动设置集群里所有集群的ip到连接客户端，它会自动帮你添加
                .build();
        try {
            this.transportAddress = new TransportAddress(InetAddress.getByName(this.host), this.port);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.transportClient = new PreBuiltTransportClient(this.settings).addTransportAddress(this.transportAddress);

        if (StringUtils.isNotBlank(this.index))
            this.requestBuilder = this.transportClient.prepareSearch(this.index);
    }

    private void initHighLevelClient() {
        if (this.port == 9200) {
            HttpHost httpHost = new HttpHost(this.host, this.port, "http");
            this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(httpHost));
        }
    }

}
