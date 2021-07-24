package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @author yhm
 * @create 2021-07-16 20:11
 */
public class Flink12_timer_exer {
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
            private Integer lastVc = Integer.MIN_VALUE;
            //声明一个变量用来初始化定时器（保存定时器时间）
            private Long timer = Long.MIN_VALUE;

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //1.判断当前水位是否大于上次水位值
                if (value.getVc() > lastVc) {
                    if (timer == Long.MIN_VALUE) {
                        System.out.println("注册定时器");
                        timer = ctx.timerService().currentProcessingTime() + 5000;
                        ctx.timerService().registerProcessingTimeTimer(timer);
                    }
                } else {
                    System.out.println("取消定时器");
                    ctx.timerService().deleteProcessingTimeTimer(timer);
                    timer = Long.MIN_VALUE;
                }
                lastVc = value.getVc();
                out.collect(value);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                //将报警信息输出到侧输出流
                ctx.output(new OutputTag<String>("output") {
                }, "报警！！" + ctx.getCurrentKey());
                timer = Long.MIN_VALUE;
            }
        });
        process.print("主流");

        process.getSideOutput(new OutputTag<String>("output") {
        }).print("报警信息:");
        env.execute();
    }
}
