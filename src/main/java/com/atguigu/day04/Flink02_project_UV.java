package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @author yhm
 * @create 2021-07-16 18:14
 */
public class Flink02_project_UV {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> userBehaviorSingleOutputStreamOperator = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
            }
        });
        // 组成Tuple2元组，keyby，sum，print
        KeyedStream<Tuple2<String, Long>, Tuple> pv = userBehaviorSingleOutputStreamOperator.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        }).map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", value.getUserId());
            }
        }).keyBy(0);
        pv.process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String,Long>>() {
            HashSet<Long> userid = new HashSet<>();
            @Override
            public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                userid.add(value.f1);
                out.collect(Tuple2.of("pv",(long)userid.size()));
            }
        }).print();
        env.execute();
    }
}
