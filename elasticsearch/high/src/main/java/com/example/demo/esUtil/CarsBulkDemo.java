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
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

/**
 * 批量新增文档 根据官方文档数据
 * https://www.elastic.co/guide/cn/elasticsearch/guide/current/_aggregation_test_drive.html
 * @author liangzhe
 * @date 2020/4/11 15:00
 */
public class CarsBulkDemo {
    private static Logger logger = LogManager.getRootLogger();

    private static final String INDEX = "cars";

    public static void main(String[] args) {
        RestHighLevelClient client = EsClient.getClient();

        BulkRequest request = new BulkRequest(INDEX);
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 10000, \"color\" : \"red\", \"make\" : \"honda\", \"sold\" : \"2014-10-28\" }", XContentType.JSON
                )
        );
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 20000, \"color\" : \"red\", \"make\" : \"honda\", \"sold\" : \"2014-11-05\" }", XContentType.JSON
                )
        );
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 30000, \"color\" : \"green\", \"make\" : \"ford\", \"sold\" : \"2014-05-18\" }", XContentType.JSON
                )
        );
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 15000, \"color\" : \"blue\", \"make\" : \"toyota\", \"sold\" : \"2014-07-02\" }", XContentType.JSON
                )
        );
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 12000, \"color\" : \"green\", \"make\" : \"toyota\", \"sold\" : \"2014-08-19\" }", XContentType.JSON
                )
        );
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 20000, \"color\" : \"red\", \"make\" : \"honda\", \"sold\" : \"2014-11-05\" }", XContentType.JSON
                )
        );
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 80000, \"color\" : \"red\", \"make\" : \"bmw\", \"sold\" : \"2014-01-01\" }", XContentType.JSON
                )
        );
        request.add(new IndexRequest(INDEX)
                .source("{ \"price\" : 25000, \"color\" : \"blue\", \"make\" : \"ford\", \"sold\" : \"2014-02-12\" }", XContentType.JSON
                )
        );


        BulkResponse response = null;
        try {
            response = client.bulk(request, RequestOptions.DEFAULT);
        } catch (
                IOException e) {
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
