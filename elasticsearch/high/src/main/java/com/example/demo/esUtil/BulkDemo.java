package com.example.demo.esUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.Index;

import java.io.IOException;

/**
 * 批量新增文档
 *
 * @author liangzhe
 * @date 2020/4/11 15:00
 */
public class BulkDemo {
    private static Logger logger = LogManager.getRootLogger();

    private static final String INDEX = "demo_index";

    public static void main(String[] args) {
        RestHighLevelClient client = EsClient.getClient();

        BulkRequest request = new BulkRequest(INDEX);
        request.add(new IndexRequest(INDEX).id("1")
                .source("user", "jack")
        );
        request.add(new IndexRequest(INDEX).id("2")
                .source("postDate", "2020-04-02")
        );
        request.add(new IndexRequest(INDEX).id("3")
                .source("message", "today is a happy day")
        );

        BulkResponse response = null;
        try {
            response = client.bulk(request, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error(e);
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }
        if (response != null) {
            for (BulkItemResponse bulkItemResponse : response) {
                DocWriteResponse docWriteResponse = bulkItemResponse.getResponse();
                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX) {
                    // 多次执行也只是匹配到的type为新增，其实操作为update
                    IndexResponse indexResponse = (IndexResponse) docWriteResponse;
                    logger.info("新增成功id:{}", indexResponse.getId());
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    UpdateResponse updateResponse = (UpdateResponse) docWriteResponse;
                    logger.info("更新成功id:{}", updateResponse.getId());
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    DeleteResponse deleteResponse = (DeleteResponse) docWriteResponse;
                    logger.info("删除成功id{}", deleteResponse.getId());
                }
            }
        }

    }

}
