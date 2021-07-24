package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author yhm
 * @create 2021-07-21 16:36
 */
public class Flink12_SQL_ProcessTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
      tableEnv.executeSql("create table sensor(id string,ts bigint,vc int, pt as proctime()) with (" +
              "'connector'='filesystem'," +
              "'path'='input/sensor.txt'," +
              "'format'='csv'" +
              ")");
      tableEnv.executeSql("select * from sensor").print();


    }
}
