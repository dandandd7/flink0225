package com.atguigu.day04;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yhm
 * @create 2021-07-16 20:11
 */
public class Flink12_Window_Session_Dynamic {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 9998);
        KeyedStream<WaterSensor,Tuple> id = source.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        }).keyBy("id");

        WindowedStream<WaterSensor, Tuple, TimeWindow> window = id.window(ProcessingTimeSessionWindows.withDynamicGap(new SessionWindowTimeGapExtractor<WaterSensor>() {
            @Override
            public long extract(WaterSensor element) {
                return element.getTs() * 1000;
            }
        }));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        window.process(new ProcessWindowFunction<WaterSensor,WaterSensor, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<WaterSensor> elements, Collector<WaterSensor> out) throws Exception {
                String start = sdf.format(new Date(context.window().getStart()));
                String end = sdf.format(new Date(context.window().getEnd()));
                System.out.println("窗口开始时间：" + start);
                System.out.println("窗口结束时间：" + end);
                for (WaterSensor element : elements) {
                    out.collect(element);
                }
            }
        }).print();
        env.execute();
    }
}
