package com.example.demo.esUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

/**
 * 新增文档
 * @author liangzhe
 * @date 2020/4/11 10:54
 */
public class IndexDocumentDemo {
    private static Logger logger = LogManager.getRootLogger();

    private static final String INDEX = "demo_index";

    public static void main(String[] args) {
        RestHighLevelClient client = EsClient.getClient();
        // 创建索引
        IndexRequest request = new IndexRequest(INDEX).id("1");

        // 1.String方式添加数据
        String jsonString = "{" +
                "\"user\":\"devin\"," +
                "\"postDate\":\"2020-04-01\"," +
                "\"message\":\"today is a lucky day\"" +
                "}";
        request.source(jsonString, XContentType.JSON);

        /* 2.通过map设置数据
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("user", "devin");
        jsonMap.put("postDate", "2020-04-01");
        jsonMap.put("message", "today is a lucky day");
        request.source(jsonMap);
          */

        /* 3.通过Builder设置数据
        try {
            XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
            xContentBuilder.startObject();
            xContentBuilder.field("user", "devin");
            xContentBuilder.field("postDate", "2020-04-01");
            xContentBuilder.field("message", "today is a lucky day");
            xContentBuilder.endObject();
            request.source(xContentBuilder);

        } catch (IOException e) {
            e.printStackTrace();
        }
         */

        /* 4.直接使用key-value
         request.source(
         "user","devin",
         "postDate", "2020-04-01",
         "message","today is a lucky day"
         );
          */

            /* 其他的一些可选设置
            request.routing("routing");  //设置routing值
            request.timeout(TimeValue.timeValueSeconds(1));  //设置主分片等待时长
            request.setRefreshPolicy("wait_for");  //设置重刷新策略
            request.version(2);  //设置版本号
            request.opType(DocWriteRequest.OpType.CREATE);  //操作类别
            */
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(request, RequestOptions.DEFAULT);
        } catch (ElasticsearchException e) {
            if (e.status().equals(RestStatus.CONFLICT)) {
                logger.error("文档已存在冲突");
            }
            logger.error("创建文档异常", e);
        }catch (IOException e){
            logger.error(e);
        }finally {
            try {
                client.close();
            } catch (IOException e) {
                logger.error(e);
            }
        }

        // 处理响应
        if(indexResponse != null) {
            String index = indexResponse.getIndex();
            String id = indexResponse.getId();
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                System.out.println("新增文档成功!");
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("修改文档成功!");
            }
            System.out.println("index:" + index);
            System.out.println("id:" + id);
        }

    }
}
