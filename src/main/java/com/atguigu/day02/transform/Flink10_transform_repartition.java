package com.atguigu.day02.transform;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-13 20:47
 */
public class Flink10_transform_repartition {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<String> datasource = env.socketTextStream("hadoop102", 9999);
        SingleOutputStreamOperator<String> map = datasource.map(r -> r).setParallelism(2);
        //-------------------
        map.print("原始数据:").setParallelism(2);
        map.keyBy(r -> r).print("KeyBy:");
        map.shuffle().print("Suffle:");
        map.rebalance().print("rebalance:");
        map.rescale().print("rescale:");


        env.execute();
    }
}
