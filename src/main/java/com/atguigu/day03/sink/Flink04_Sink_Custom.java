package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @author yhm
 * @create 2021-07-14 18:51
 */
public class Flink04_Sink_Custom {
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

        map.addSink(new MyJDBC());

        env.execute();
    }

    public static class MyJDBC implements SinkFunction<WaterSensor>{
        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            final Connection connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            final PreparedStatement ps = connection.prepareStatement("insert into sensor values (?,?,?)");
            ps.setString(1,value.getId());
            ps.setLong(2,value.getTs());
            ps.setInt(3,value.getVc());
            ps.execute();

            ps.close();
            connection.close();
        }
    }

}
