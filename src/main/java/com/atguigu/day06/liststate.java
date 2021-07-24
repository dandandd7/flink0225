package com.atguigu.day06;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.swing.tree.VariableHeightLayoutCache;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * @author yhm
 * @create 2021-07-19 11:41
 */
public class liststate<I extends Number> {
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
        //针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, List<Integer>>() {
            private ListState<Integer> vcState;

            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getListState(new ListStateDescriptor<Integer>("vcstate", Integer.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<List<Integer>> out) throws Exception {
                vcState.add(value.getVc());
                //获取水位高度 并排序
                Iterable<Integer> integers = vcState.get();
                ArrayList<Integer> vcs = new ArrayList<>();

                for (Integer integer : integers) {
                    vcs.add(integer);
                }


                //降序
                vcs.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });
                //超过3个时，去掉最后一个
                if (vcs.size() > 3) {
                    vcs.remove(3);
                }
                vcState.update(vcs);
                out.collect(vcs);
            }
        }).print();


        env.execute();
    }
}
