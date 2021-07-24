package com.atguigu.day06;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author yhm
 * @create 2021-07-19 11:41
 */
public class aggstate<I extends Number> {
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
        //计算每个传感器的平均水位
       keyedStream.process(new KeyedProcessFunction<Tuple , WaterSensor, Tuple2<String,Double>>() {
           private AggregatingState<Integer,Double> avgState;
           @Override
           public void open(Configuration parameters) throws Exception {
               avgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, Tuple2<Integer,Integer>,Double>("avg-state", new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                   @Override
                   public Tuple2<Integer, Integer> createAccumulator() {
                       return Tuple2.of(0,0);
                   }

                   @Override
                   public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                       return Tuple2.of(value+accumulator.f0,accumulator.f1+1);
                   }

                   @Override
                   public Double getResult(Tuple2<Integer, Integer> accumulator) {
                       return accumulator.f0*1D/accumulator.f1;
                   }

                   @Override
                   public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                       return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                   }
               }, Types.TUPLE(Types.INT,Types.INT)));
           }

           @Override
           public void processElement(WaterSensor value, Context ctx, Collector<Tuple2<String,Double>> out) throws Exception {
                avgState.add(value.getVc());
                out.collect(Tuple2.of(value.getId(),avgState.get()));
           }
       }).print();

        env.execute();
    }
}
