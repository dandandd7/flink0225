package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-13 19:56
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/word.txt");
        stringDataStreamSource.print();
        env.execute();
    }
}
