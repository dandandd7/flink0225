package com.atguigu.day06;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
public class mapstate<I extends Number> {
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
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            private MapState<Integer,WaterSensor> mapState;
            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("map-state",Integer.class, WaterSensor.class));
            }
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                if (!mapState.contains(value.getVc())) {
                    out.collect(value);
                    mapState.put(value.getVc(),value);
                }
            }
        }).print();

        env.execute();
    }
}
