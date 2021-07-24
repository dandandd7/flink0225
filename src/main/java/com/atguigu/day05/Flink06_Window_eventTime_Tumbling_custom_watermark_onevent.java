package com.atguigu.day05;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * @author yhm
 * @create 2021-07-16 20:11
 */
public class Flink06_Window_eventTime_Tumbling_custom_watermark_onevent {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000);
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9998);
        //
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //分配watermark
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.assignTimestampsAndWatermarks(new WatermarkStrategy<WaterSensor>() {
            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyWaterMarkOnperiodic(Duration.ofSeconds(2));
            }
        }
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs()*1000;
                    }
                })).keyBy(WaterSensor::getId);
        //先开启滚动窗口
        keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5))).process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前Key：" + key +"窗口：[" +context.window().getStart()/1000+","+context.window().getEnd()/1000+"),一共有"+elements.spliterator().estimateSize()+"条数据";
                out.collect(msg);
            }
        }).print();

        env.execute();
    }
    public static class MyWaterMarkOnperiodic implements WatermarkGenerator<WaterSensor>{
        /** The maximum timestamp encountered so far. */
        private long maxTimestamp;

        /** The maximum out-of-orderness that this watermark generator assumes. */
        private  long outOfOrdernessMillis;

        public MyWaterMarkOnperiodic(Duration maxOutOfOrderness) {
            checkNotNull(maxOutOfOrderness, "maxOutOfOrderness");
            checkArgument(!maxOutOfOrderness.isNegative(), "maxOutOfOrderness cannot be negative");

            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

            // start so that our lowest watermark would be Long.MIN_VALUE.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("生成WaterSensor");
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {


        }
    }
}
