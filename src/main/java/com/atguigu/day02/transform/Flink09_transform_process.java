package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import jdk.nashorn.internal.runtime.regexp.joni.WarnCallback;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-14 18:03
 */
public class Flink09_transform_process {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> dsSource = env.socketTextStream("hadoop102", 9999);
        final SingleOutputStreamOperator<WaterSensor> process = dsSource.process(new ProcessFunction<String, WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                final String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });
        final KeyedStream<WaterSensor, Tuple> keyedStream = process.keyBy("id");
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                out.collect(new WaterSensor(value.getId() + "process", value.getTs() + System.currentTimeMillis(), value.getVc() + 1000));
            }
        }).print();
        env.execute();
    }
}
