package com.atguigu.day04;

/**
 * @author yhm
 * @create 2021-07-16 21:22
 */
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Flink11_Window_Session_withStartandEnd {
    public static void main(String[] args) throws Exception {
        //1.流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9998);

        //3.对数据进行处理，封装成Tuple元组
        SingleOutputStreamOperator<String> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(value);
            }

        });

        //4.将相同元素的数据聚和到一块
        KeyedStream<String, String> keyedStream = wordToOneDStream.keyBy(r->r);

        //TODO 5.开启基于时间会话窗口 ->会话间隔5S
        WindowedStream<String, String, TimeWindow> window = keyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        window.process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                String start = sdf.format(new Date(context.window().getStart()));
                String end = sdf.format(new Date(context.window().getEnd()));
                System.out.println("窗口开始时间：" + start);
                System.out.println("窗口结束时间：" + end);
                for (String element : elements) {
                    out.collect(element);
                }
            }
        }).print();

        env.execute();
    }
}