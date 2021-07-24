package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-12 11:36
 */
public class TestBatch {
    public static void main(String[] args) throws Exception {
        //获取批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //获取数据
        final DataSource<String> dataSource = env.readTextFile("input/word.txt");
        //设置并行度为1
        dataSource.setParallelism(1);
        //开始处理
        //flatmap
        final FlatMapOperator<String, Tuple2<String, Long>> stringTuple2FlatMapOperator = dataSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                        final String[] strings = s.split(" ");
                        for (String string : strings) {
                            collector.collect(Tuple2.of(string, 1L));
                        }
                    }
                });
        //将相同的key聚合在一起求和
        final UnsortedGrouping<Tuple2<String, Long>> tuple2UnsortedGrouping = stringTuple2FlatMapOperator.groupBy(0);
        final AggregateOperator<Tuple2<String, Long>> sum = tuple2UnsortedGrouping.sum(1);
        sum.print();
    }
}
