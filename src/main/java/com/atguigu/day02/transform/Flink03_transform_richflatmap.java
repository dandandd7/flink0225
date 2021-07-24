package com.atguigu.day02.transform;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.protocol.types.Field;

/**
 * @author yhm
 * @create 2021-07-13 21:09
 */
public class Flink03_transform_richflatmap {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> datasource = env.socketTextStream("hadoop102", 9000);
        datasource.flatMap(new Myrichflatmap()).print();
        env.execute();
    }
    public static class Myrichflatmap extends RichFlatMapFunction<String,String> {
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("start.....");
        }

        @Override
        public void close() throws Exception {
            System.out.println("close.....");
        }

        public void flatMap(String value, Collector<String> out) throws Exception {
            final String[] split = value.split(",");
            for (String s : split) {
                out.collect(s);
            }
        }
    }
}
