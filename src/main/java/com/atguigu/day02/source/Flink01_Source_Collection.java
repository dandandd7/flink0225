package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author yhm
 * @create 2021-07-13 19:52
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final List<String> list = Arrays.asList("1", "2", "3", "4");
        final DataStreamSource<String> stringDataStreamSource = env.fromCollection(list);
        stringDataStreamSource.print();
        env.execute();
    }
}
