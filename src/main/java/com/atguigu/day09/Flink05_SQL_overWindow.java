package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2021-07-21 16:36
 */
public class Flink05_SQL_overWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 创建表的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().getConfiguration().setString("table.local-time-zone","GMT");
      tableEnv.executeSql("create table sensor(id string,ts bigint,vc int, " +
              "t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss'))," +
              "watermark for t as t - interval '5' second" +
              ") with (" +
              "'connector'='filesystem'," +
              "'path'='input/sensor-sql.txt'," +
              "'format'='csv'" +
              ")");
      //3.查询表并使用开窗函数 方式1
//      tableEnv.executeSql("select id,ts,vc,\n" +
//              "sum(vc) over w \n" +
//              "from sensor \n" +
//              "window w as(partition by id order by t)").print();
        //3.查询表并使用开窗函数 方式2
        tableEnv.executeSql("select id,ts,vc," +
                "sum(vc) over (partition by id order by t)," +
                "count(vc) over (partition by id order by t)" +
                "from sensor").print();

    }
}
