package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-14 16:53
 */
public class Flink08_transform_reduce {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> source = env.socketTextStream("hadoop102", 9999);
        final SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
            public WaterSensor map(String value) throws Exception {
                final String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        final KeyedStream<WaterSensor, Object> keyed = map.keyBy(k -> k.getId());
        keyed.reduce(new ReduceFunction<WaterSensor>() {
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reduce......");
                return new WaterSensor(value2.getId(),value2.getTs(),Math.max(value1.getVc(),value2.getVc()));
            }
        }).print();
        env.execute();


    }
}
