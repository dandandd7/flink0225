package com.atguigu.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-17 14:03
 */
public class Flink16_Window_time_tumbling_agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9998);

        //3.对数据进行处理，封装成Tuple元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordToOneDStream = streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                out.collect(Tuple2.of(value, 1L));
            }
        });
        KeyedStream<Tuple2<String, Long>, Tuple> wordToOneDStream1 = wordToOneDStream.keyBy(0);
        wordToOneDStream1
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Tuple2<String, Long>, Long, Long>() {
                    @Override
                    //初始化累加器
                    public Long createAccumulator() {
                        System.out.println("初始化累加器");
                        return 0L;
                    }

                    @Override
                    //累加操作
                    public Long add(Tuple2<String, Long> value, Long accumulator) {
                        System.out.println("累加操作");
                        return value.f1+accumulator;
                    }

                    @Override
                    //获取结果集
                    public Long getResult(Long accumulator) {
                        System.out.println("获取结果集");
                        return accumulator;
                    }

                    @Override
                    //合并
                    public Long merge(Long a, Long b) {
                        System.out.println("合并");
                        return a+b;
                    }
                }).print();

        env.execute();

    }
}
