package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-13 20:47
 */
public class Flink02transform_flatmap {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> datasource = env.socketTextStream("hadoop102", 9998);
        final SingleOutputStreamOperator<String> map = datasource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                final String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });
        map.print();
        env.execute();
    }
}
