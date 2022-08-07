package com.example.springbootes;


import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
public class FlightIndexSearcherTestor {
    @Test
    public void qury() throws IOException {
        RestHighLevelClient client = null;
        try{
            client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost",9200)));

            //构建一个多条件的bool查询
            BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
            //第一个是字段名，第二个是输入,查询的类型是termQuery,精确查询
            boolQueryBuilder.must(QueryBuilders.termQuery("OriginCityName","Venice"));
            boolQueryBuilder.must(QueryBuilders.termQuery("DestCountry","CN"));

            //利用searchsourcebuilder 构建附加选项，如排序、分页等
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

            searchSourceBuilder.query(boolQueryBuilder);

            searchSourceBuilder.from(0);
            searchSourceBuilder.size(10);
            //开启命中统计，这一行必须设置，因为默认为false，且es记录上限为1w
            searchSourceBuilder.trackTotalHits(true);
            searchSourceBuilder.sort("AvgTicketPrice", SortOrder.ASC);

            //构建请求
            SearchRequest request = new SearchRequest("kibana_sample_data_flights");
            request.source(searchSourceBuilder);

            SearchResponse response = client.search(request, RequestOptions.DEFAULT);

            //得到命中文档的集合
            SearchHit[] hits = response.getHits().getHits();

            //引入反序列化工具
            Gson gson = new Gson();
            List<Map<String,Object>> list = new ArrayList<>();
            for (SearchHit hit:hits){
                String json = hit.getSourceAsString();
                //fromJson用来接收返回的数据，TypeToken
                Map<String,Object> doc = gson.fromJson(json,new TypeToken<LinkedHashMap<String,Object>>(){}.getType());
                list.add(doc);
            }

            Long count = response.getHits().getTotalHits().value;

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (client != null){
                client.close();
            }
        }

    }
}
