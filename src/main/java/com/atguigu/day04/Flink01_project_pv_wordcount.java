package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-16 11:15
 */
public class Flink01_project_pv_wordcount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile("input/UserBehavior.csv");
        SingleOutputStreamOperator<UserBehavior> map = dataStreamSource.map(new MapFunction<String, UserBehavior>() {
            @Override
            public UserBehavior map(String value) throws Exception {
                String[] split = value.split(",");
                return new UserBehavior(Long.valueOf(split[0]),
                        Long.valueOf(split[1]), Integer.valueOf(split[2]),
                        split[3], Long.valueOf(split[4]));
            }
        });
        SingleOutputStreamOperator<UserBehavior> filter = map.filter(new FilterFunction<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior value) throws Exception {
                return "pv".equals(value.getBehavior());
            }
        });
        //过滤好之后  组成Tuple2元组，keyby，sum，print
        filter.map(new MapFunction<UserBehavior, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return new Tuple2("pv",1L);
            }
        }).keyBy(0).sum(1).print();
        env.execute();
    }
}
