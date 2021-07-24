package com.atguigu.day02.transform;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author yhm
 * @create 2021-07-14 11:32
 */
public class Flink05_transform_connect {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> ds1 = env.fromElements("1", "2", "3", "4", "5");
        final DataStreamSource<Integer> ds2 = env.fromElements(100, 200, 300, 400, 500);
        final ConnectedStreams<String, Integer> connectedStreams = ds1.connect(ds2);
        /*final SingleOutputStreamOperator<String> map = connectedStreams.map(new CoMapFunction<String, Integer, String>() {
            public String map1(String value) throws Exception {
                return value + "map1";
            }

            public String map2(Integer value) throws Exception {
                return "map2" + value;
            }
        });*/
        connectedStreams.getFirstInput().print("first");
        connectedStreams.getSecondInput().print("second");
        env.execute();
    }
}
