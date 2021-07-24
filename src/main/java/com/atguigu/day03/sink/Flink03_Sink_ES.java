package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;

/**
 * @author yhm
 * @create 2021-07-14 18:51
 */
public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9998);
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                final String[] split = value.split(",");

                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        ArrayList<HttpHost> httpHosts = new ArrayList<HttpHost>();
        httpHosts.add(new HttpHost("hadoop102",9200));
        httpHosts.add(new HttpHost("hadoop103",9200));
        httpHosts.add(new HttpHost("hadoop104",9200));

        final ElasticsearchSink.Builder<WaterSensor> waterSensorBuilder = new ElasticsearchSink.Builder<WaterSensor>(httpHosts, new ElasticsearchSinkFunction<WaterSensor>() {
            @Override
            public void process(WaterSensor waterSensor, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                final IndexRequest doc = new IndexRequest("sensor-0225", "_doc", waterSensor.getId());
                doc.source(JSON.toJSONString(waterSensor), XContentType.JSON);
                requestIndexer.add(doc);
            }
        });
        waterSensorBuilder.setBulkFlushMaxActions(1);
        map.addSink(waterSensorBuilder.build());

        env.execute();
    }

}
