package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-12 13:42
 */
public class Bounded {
    public static void main(String[] args) throws Exception {
        //流处理执行环境
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> stringDataStreamSource = senv.readTextFile("input/word.txt");
        senv.setParallelism(1);
        final SingleOutputStreamOperator<Tuple2<String, Long>> tuple2SingleOutputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                final String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(Tuple2.of(s2, 1L));
                }
            }
        });
        final SingleOutputStreamOperator<Tuple2<String, Long>> sum = tuple2SingleOutputStreamOperator.keyBy(0).sum(1);
        sum.print();
        senv.execute();

    }
}
