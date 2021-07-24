package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-16 11:32
 */
public class Flink02_project_pv_process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");
        dataStreamSource.process(new ProcessFunction<String, Tuple2<String, Long>>() {
            long count = 0;
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                if ("pv".equals(value.split(",")[3])) {
                    out.collect(Tuple2.of("pv", ++count));
                }
            }
        }).print();
        env.execute();
        //过滤 元组 keyby sum print
    }
}
