package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-19 11:41
 */
public class valuestate<I extends Number> {
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
        KeyedStream<WaterSensor, String> keyedStream = map.keyBy(WaterSensor::getId);
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            private ValueState<Integer> state;

            @Override
            public void open(Configuration parameters) throws Exception {
                state = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                int lastVc = state.value() == null ? 0 : state.value();
                if(Math.abs(value.getVc()-lastVc)>=10){
                    out.collect(value.getId()+"红色警报。。");
                }
                state.update(value.getVc());
            }
        }).print();
        env.execute();
    }
}
