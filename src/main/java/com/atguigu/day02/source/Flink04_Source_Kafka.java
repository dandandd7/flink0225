package com.atguigu.day02.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author yhm
 * @create 2021-07-13 20:10
 */
public class Flink04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.setProperty("group.id", "Flink04_Source_Kafka");
        props.setProperty("auto.offset.reset", "latest");

        final DataStreamSource<String> sensor = env.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), props));
        sensor.print();
        env.execute();
    }
}
