package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author yhm
 * @create 2021-07-21 16:14
 */
public class Flink05_TableApi_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Schema schema = new Schema().field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor")
                .startFromLatest()
                .property("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .property("group.id", "ayguigu")
        ).withFormat(new Json())
                .withSchema(schema)
                .createTemporaryTable("sensor");
        //对动态表进行查询
        Table table = tableEnv.from("sensor");
        Table select = table.groupBy($("id"))
                .select($("id"), $("id").count().as("cnt"));
        tableEnv.toRetractStream(select,Row.class).print();
        env.execute();
    }
}
