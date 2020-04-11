package com.example.demo.esUtil;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * esClient
 * @author liangzhe
 * @date 2020/4/10 20:39
 */
public class EsClient {


    private static int port = 9200;
    private static String host = "127.0.0.1";   //可以写 ip  但是 logstash 配置要修改

    public static RestHighLevelClient getClient() {
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                                new HttpHost(host, port, "http")
                        // 可以指定集群的多个节点地址，将客户端负载均衡地向这个节点地址集发请求
                        ));
        return client;
    }

}