package es;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * @author admin 2020-6-19
 */
public class EsQueryDemo {

    public SearchRequestBuilder setRequestByQuery(SearchRequestBuilder requestBuilder, QueryBuilder queryBuilder) {
        return requestBuilder.setQuery(queryBuilder);
    }

    private void baseQuery(QueryBuilder qb) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery(); // 综合查询
        /* URI 搜索*/
        String query = "field1:v1 AND field1:v2";
        query = "field1:v1 + field1:v2";
        qb = QueryBuilders.queryStringQuery(query);
        /* DSL 标准查询*/
        // 短语查询
        qb = QueryBuilders.matchPhraseQuery("fieldName", "values");
        boolQuery.must(qb);
        // 匹配查询
        qb = QueryBuilders.matchQuery("fieldName", "matchValue");

        qb = QueryBuilders.rangeQuery("publishDate").gt("2018-01-01");
        boolQuery.filter(qb);   // range限制范围
        boolQuery.should(qb);   // should提高得分
    }

    private void actionQuery(SearchRequestBuilder srb) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags("<h2>").postTags("</h2>");
        highlightBuilder.field("fieldName");
        srb.highlighter(highlightBuilder);  // 高亮显示

        srb.setFetchSource(new String[]{"field1", "field2"}, null); // 指定查询字段

        srb.setFrom(0).setSize(2);  // 分页
        // sort: date, num, keyword, text.keyword(不分词), text.fielddata:true(分词)
        srb.addSort("publishDate", SortOrder.DESC);
    }

}
