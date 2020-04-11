package com.example.demo.esUtil;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * 获取文档
 * @author liangzhe
 * @date 2020/4/11 14:45
 */
public class GetDocumentDemo {
    private static Logger logger = LogManager.getRootLogger();

    private static final String INDEX = "demo_index";

    public static void main(String[] args) {
        RestHighLevelClient client = EsClient.getClient();

        GetRequest request = new GetRequest(INDEX).id("1");
        GetResponse response = null;
        try {
            response = client.get(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {
                logger.error("没有找到文档");
            }
        } catch (IOException e) {
            logger.error("获取文档异常", e);
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }

        if (response != null) {
            String index = response.getIndex();
            String id = response.getId();
            if (response.isExists()) {
                String sourceAsString = response.getSourceAsString();
                logger.info("index:" + index + "  id:" + id);
                logger.info(sourceAsString);
            }
        }

    }
}
