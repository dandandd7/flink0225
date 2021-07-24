package com.atguigu.day08;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author yhm
 * @create 2021-07-21 12:55
 */
public class Flink04_TableAPI_Connect_agg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Schema schame = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("vc",DataTypes.INT());
        //做成临时表
        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schame)
                .createTemporaryTable("sensor");
        //做成表对象   动态查询
        Table sensor = tableEnv.from("sensor");
        Table select = sensor
                .groupBy($("id"))
                .aggregate($("id").count().as("idct"))
                .select($("id"), $("idct"));
//通过调用execute（）返回一个tableresult类型可以直接打印
        select.execute().print();

    }
}
