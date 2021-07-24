package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author yhm
 * @create 2021-07-21 11:38
 */
public class Flink02_TableApi_BasicUse_agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> streamSource = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_2", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_2", 6000L, 60));
        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.将流转换为动态表
        Table table = tableEnv.fromDataStream(streamSource);
        //3.对动态表进行查询
        /*Table select = table
                .groupBy($("id"))
                .select($("id"),  $("vc").sum().as("VcSum" ));*/
        Table select = table
                .groupBy($("id"))
                .aggregate($("vc").sum().as("vcsum"))
                .select($("id"), $("vcsum"));
        //4.吧动态表转换成流
        tableEnv.toRetractStream(select, Row.class).print();
        env.execute();

    }
}
