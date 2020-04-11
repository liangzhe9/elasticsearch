package com.example.demo.esUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;

/**
 * 创建索引
 * @author liangzhe
 * @date 2020/4/10 20:45
 */
public class CreateIndexDemo {
    private static Logger logger = LogManager.getRootLogger();

    public static void main(String[] args) {

        RestHighLevelClient client = EsClient.getClient();
        // 创建索引
//        CreateIndexRequest request = new CreateIndexRequest("demo_index");
        CreateIndexRequest request = new CreateIndexRequest("cars");
        // 索引setting配置
        request.settings(Settings.builder().put("index.number_of_shards", 5)
                .put("index.number_of_replicas", 2) // 副本数
                .put("analysis.analyzer.default.tokenizer", "standard")
        );

//        request.mapping("{" +
//                "\"properties\": {" +
//                "\"commodity_id\": {" +
//                "\"type\": \"integer\"" +
//                "}," +
//                "\"commodity_name\": {" +
//                "\"type\": \"text\"" +
//                "}," +
//                "\"picture_url\": {" +
//                "\"type\": \"text\"" +
//                "}," +
//                "\"price\": {" +
//                "\"type\": \"double\"" +
//                "}" +
//                "}" +
//                "}", XContentType.JSON);

        CreateIndexResponse response = null;
        try {
            response = client.indices().create(request, RequestOptions.DEFAULT);

            if (response != null) {
                boolean acknowledged = response.isAcknowledged();
                boolean shardsAcknowledged = response.isShardsAcknowledged();

                System.out.println("请求结果---------------");
                System.out.println("acknowledged:" + acknowledged);
                System.out.println("shardsAcknowledged:" + shardsAcknowledged);
            }
        } catch (IOException e) {
            logger.error("创建index失败", e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }
    }
}
