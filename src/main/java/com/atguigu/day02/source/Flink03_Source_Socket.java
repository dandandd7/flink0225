package com.atguigu.day02.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-13 19:58
 */
public class Flink03_Source_Socket {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> nc = env.socketTextStream("hadoop102", 9999);
        nc.print();
        env.execute();
    }
}
