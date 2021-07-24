package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-14 11:50
 */
public class Flink07_transform_agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            public WaterSensor map(String value) throws Exception {
                final String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        final KeyedStream<WaterSensor, Object> waterSensorObjectKeyedStream = map.keyBy(new KeySelector<WaterSensor, Object>() {
            public Object getKey(WaterSensor r) throws Exception {
                return r.getId();
            }
        });
//        waterSensorObjectKeyedStream.max("vc").print();
        waterSensorObjectKeyedStream.maxBy("vc").print();
        env.execute();
    }
}
