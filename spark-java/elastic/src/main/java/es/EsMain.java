package es;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * demo {@link EsQueryDemo}
 *
 * @author admin 2020-6-16
 */
public class EsMain {
    public static void main(String[] args) {
        EsClient esClient = new EsClient("customer");
        Client client = esClient.getTransportClient();
        SearchRequestBuilder srb = esClient.getRequestBuilder();

        SearchRequestBuilder requestBuilder = srb.setQuery(QueryBuilders.matchAllQuery())
                .addSort("name.keyword", SortOrder.DESC);

        foreachHitsByRequest(requestBuilder);
        client.close();
        //dmLevelClient();
    }

    private static void dmLevelClient() {
        RestHighLevelClient levelClient = new EsClient("localhost", 9200).getRestHighLevelClient();
        IndexRequest indexRequest = new IndexRequest("customer").id("1");
        // 插入文档 result: created/update
        indexRequest.source("{\"name\":\"湖南\"}", XContentType.JSON);
        // 删除指定文档，id不存在 result：NotFound
        DeleteRequest deleteRequest = new DeleteRequest("customer", "ykXXynIB9Rc-nRRaDENz");
        try {
            //IndexResponse indexResponse = levelClient.index(indexRequest, RequestOptions.DEFAULT);
            DeleteResponse delete = levelClient.delete(deleteRequest, RequestOptions.DEFAULT);
            SearchResponse customer = levelClient.search(new SearchRequest("customer"), RequestOptions.DEFAULT);
            foreachHitsResponse(customer);
            levelClient.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void foreachHitsByRequest(SearchRequestBuilder requestBuilder) {
        SearchResponse response = requestBuilder.execute().actionGet();
        foreachHitsResponse(response);
    }

    private static void foreachHitsResponse(SearchResponse response) {
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            String source = hit.getSourceAsString();
            System.out.println(source);
            JSONObject jsonObject = JSON.parseObject(source);
//            jsonObject.toJavaObject(Object.class);
        }
    }
}
