package com.atguigu.day02.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-14 11:41
 */
public class Flink06_transform_union {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<Integer> ds1 = env.fromElements(1, 2, 3, 4, 5);
        final DataStreamSource<Integer> ds2 = env.fromElements(10, 20, 30, 40, 50);
        final DataStreamSource<Integer> ds3 = env.fromElements(100, 200, 300, 400, 500);
        ds1.union(ds2,ds3).print("first");
        env.execute();

    }
}
