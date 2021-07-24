package com.atguigu.day04;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-17 14:03
 */
public class Flink17_Window_time_tumbling_process {
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
        KeyedStream<Tuple2<String, Long>, String> wordToOneDStream1 = wordToOneDStream.keyBy(new KeySelector<Tuple2<String, Long>, String>() {
            @Override
            public String getKey(Tuple2<String, Long> value) throws Exception {
                return value.f0;
            }
        });
      wordToOneDStream1.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
              .process(new ProcessWindowFunction<Tuple2<String, Long>, Long, String, TimeWindow>() {
                  Long a =0L;
                  @Override
                  public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<Long> out) throws Exception {
                      System.out.println("proocess...");
                      for (Tuple2<String, Long> element : elements) {
                          a +=element.f1;
                      }
                      out.collect(a);
                  }

              }).print();
        env.execute();

    }
}
