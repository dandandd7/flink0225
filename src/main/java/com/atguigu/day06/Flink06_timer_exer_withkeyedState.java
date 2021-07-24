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
import org.apache.flink.util.OutputTag;

/**
 * @author yhm
 * @create 2021-07-16 20:11
 */
public class Flink06_timer_exer_withkeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9998);
        //
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });


        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorSingleOutputStreamOperator.keyBy("id");
        //监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警，并将报警信息输出到侧输出流。
        SingleOutputStreamOperator<WaterSensor> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor, WaterSensor>() {
            //定义上次的水位值
            private ValueState<Integer> lastVc;
            //声明一个变量用来初始化定时器（保存定时器时间）
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vcstate",Integer.TYPE,Integer.MIN_VALUE));
                timer = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tsstate",Long.TYPE,Long.MIN_VALUE));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //1.判断当前水位是否大于上次水位值
                if (value.getVc() > lastVc.value()) {
                    if (timer.value() == Long.MIN_VALUE) {
                        System.out.println("注册定时器");
                        timer.update(ctx.timerService().currentProcessingTime() + 5000);
                        ctx.timerService().registerProcessingTimeTimer(timer.value());
                    }
                } else {
                    System.out.println("取消定时器");
                    ctx.timerService().deleteProcessingTimeTimer(timer.value());
                    timer.clear();
                }
                lastVc.update(value.getVc());
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                //将报警信息输出到侧输出流
                ctx.output(new OutputTag<String>("output") {
                }, "报警！！" + ctx.getCurrentKey());
                timer.clear();
            }
        });
        process.print("主流");

        process.getSideOutput(new OutputTag<String>("output") {
        }).print("报警信息:");
        env.execute();
    }
}
