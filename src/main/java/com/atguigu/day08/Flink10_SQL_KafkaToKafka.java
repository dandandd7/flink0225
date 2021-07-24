package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yhm
 * @create 2021-07-21 18:30
 */
public class Flink10_SQL_KafkaToKafka {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //获取表执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        //1.创建kafkaSource表: source_sensor
        tableEnvironment.executeSql("create table if not exists source_sensor(id string,ts bigint,vc int) " +
                "with(" +
                "'connector'='kafka'," +
                "'topic'='topic_source_sensor'," +
                "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "'properties.group.id'='atguigu'," +
                "'scan.startup.mode'='latest-offset'," +
                "'format'='csv'" +
                ")");

        // 2. 注册SinkTable: sink_sensor
        tableEnvironment.executeSql("create table if not exists sink_sensor(id string,ts bigint,vc int) with(" +
                "'connector'='kafka'," +
                "'topic'='topic_sink_sensor'," +
                "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                "'format'='csv'" +
                ")");

        // 3. 从SourceTable 查询数据, 并写入到 SinkTable
        tableEnvironment.executeSql("insert into sink_sensor select * from source_sensor where id = 'sensor_1'");
    }
}
