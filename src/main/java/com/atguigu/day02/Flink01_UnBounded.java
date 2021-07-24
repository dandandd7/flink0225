package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-12 13:54
 */
public class Flink01_UnBounded {
    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        final DataStreamSource<String> stringDataStreamSource = senv.socketTextStream("hadoop102", 9999);
        senv.setParallelism(1);
//        senv.disableOperatorChaining();
        final SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                final String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(Tuple2.of(s2, 1L));
                }
            }
        });
        final SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2SingleOutputStreamOperator.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            public String getKey(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return stringLongTuple2.f0;
            }
        }).sum(1);
        sum.print();

        senv.execute();
    }
}
