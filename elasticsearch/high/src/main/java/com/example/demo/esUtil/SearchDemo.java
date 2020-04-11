package com.example.demo.esUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;

/**
 * 搜索文档
 * @author liangzhe
 * @date 2020/4/11 15:58
 */
public class SearchDemo {
    private static Logger logger = LogManager.getRootLogger();

    private static final String INDEX = "demo_index";

    public static void main(String[] args) {
        RestHighLevelClient client = EsClient.getClient();

        SearchRequest request = new SearchRequest(INDEX);
        // 其实这些最后都会转化为String类型的 es query查询语句，只不过java封装了可以省却了写原生query语句
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 全匹配查询
        /*
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(matchAllQueryBuilder);
        */

        // 精确查询
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("user", "devin");
        searchSourceBuilder.query(termQueryBuilder);


        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);

        //指定排序
        /*
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));
        */

        request.source(searchSourceBuilder);
        SearchResponse response = null;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error(e);
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }
        if(response != null){
            SearchHits hits = response.getHits();
            SearchHit[] searchHits = hits.getHits();
            Arrays.stream(searchHits).forEach( h ->
                    logger.info(h.toString())
            );
        }

    }

}
