package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-13 20:47
 */
public class Flink04_transform_map_keyby {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        final DataStreamSource<String> datasource = env.socketTextStream("hadoop102", 9000);
        final SingleOutputStreamOperator<WaterSensor> map = datasource.map(new MapFunction<String, WaterSensor>() {
            public WaterSensor map(String value) throws Exception {
                final String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        final KeyedStream<WaterSensor, Tuple> id = map.keyBy("id");
        id.print();
//        map.print();
        env.execute();
    }
}
