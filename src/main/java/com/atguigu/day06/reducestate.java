package com.atguigu.day06;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author yhm
 * @create 2021-07-19 11:41
 */
public class reducestate<I extends Number> {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9998);
        SingleOutputStreamOperator<WaterSensor> map = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //键控状态都得按键分组
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");
        //计算每个传感器的水位和
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            private ReducingState<WaterSensor> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState= getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reducint-state", new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(), value2.getTs(), value1.getVc() + value2.getVc());
                    }
                }, WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                reducingState.add(value);
                //取出状态中的数据
                WaterSensor waterSensor = reducingState.get();
                //输出数据
                out.collect(waterSensor);
            }
        }).print();


        env.execute();
    }
}
