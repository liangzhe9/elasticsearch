package com.example.demo.esUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Min;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

/**
 * 聚合查询 已官方文档中的cars为例
 * https://www.elastic.co/guide/cn/elasticsearch/guide/current/_one_final_modification.html
 *
 * @author liangzhe
 * @date 2020/4/11 16:17
 */
public class AggregationDemo {
    private static Logger logger = LogManager.getRootLogger();

    private static final String INDEX = "cars";

    public static void main(String[] args) {
        RestHighLevelClient client = EsClient.getClient();

        // 1、创建search请求
        SearchRequest request = new SearchRequest(INDEX);


        // 2、用SearchSourceBuilder来构造查询请求体 ,请仔细查看它的方法，构造各种查询的方法都在这。
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);

        //加入聚合
        //字段值项分组聚合
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("colors")
                .field("color.keyword");

        // 计算通过color分组后的平均价格
        aggregation.subAggregation(
                AggregationBuilders.avg("avg_price").field("price")
        );
        aggregation.subAggregation(
                AggregationBuilders.terms("make").field("make.keyword")
                        .subAggregation(AggregationBuilders.max("max_price").field("price"))
                        .subAggregation(AggregationBuilders.min("min_price").field("price"))
        );


        searchSourceBuilder.aggregation(aggregation);

        System.out.println("----------");
        System.out.println(searchSourceBuilder.toString());
        request.source(searchSourceBuilder);

        //3、发送请求
        SearchResponse response = null;
        try {
            response = client.search(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error(e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }

        if (response != null && response.status() == RestStatus.OK) {
            Aggregations aggregations = response.getAggregations();
            Terms colors = aggregations.get("colors");
            for (Terms.Bucket bucket : colors.getBuckets()) {
                logger.info("------------------");
                logger.info("color:{}", bucket.getKeyAsString());
                Aggregations aggregations1 = bucket.getAggregations();
                Avg avg_price = aggregations1.get("avg_price");
                logger.info("avg_price:{}", avg_price.getValue());
                Terms make = bucket.getAggregations().get("make");
                for (Terms.Bucket bucket2 : make.getBuckets()) {
                    logger.info("make:{}", bucket2.getKeyAsString());
                    Max max_price = bucket2.getAggregations().get("max_price");
                    Min min_price = bucket2.getAggregations().get("min_price");
                    logger.info("max_price=" + max_price.getValue());
                    logger.info("min_price=" + min_price.getValue());
                }
            }
        }
    }

}
