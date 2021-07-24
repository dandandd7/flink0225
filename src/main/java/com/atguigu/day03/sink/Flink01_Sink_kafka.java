package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

/**
 * @author yhm
 * @create 2021-07-14 18:51
 */
public class Flink01_Sink_kafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9998);
        SingleOutputStreamOperator<String> map = dataStreamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                final String[] split = value.split(",");
                final WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                return JSONObject.toJSONString(waterSensor);
            }
        });
//        final Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers","hadoop102:9092");
//        final FlinkKafkaProducer<String> MySink = new FlinkKafkaProducer<>("sensor", new SimpleStringSchema(), properties);
        final FlinkKafkaProducer<String> sensor = new FlinkKafkaProducer<>("hadoop102:9092", "sensor", new SimpleStringSchema());
        map.addSink(sensor);
//        map.addSink(MySink);
        env.execute();
    }
}
