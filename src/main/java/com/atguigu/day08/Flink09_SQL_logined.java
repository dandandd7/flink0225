package com.atguigu.day08;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author yhm
 * @create 2021-07-21 17:59
 */
public class Flink09_SQL_logined {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<WaterSensor> waterSensorStream =
                env.fromElements(new WaterSensor("sensor_1", 1000L, 10),
                        new WaterSensor("sensor_1", 2000L, 20),
                        new WaterSensor("sensor_2", 3000L, 30),
                        new WaterSensor("sensor_1", 4000L, 40),
                        new WaterSensor("sensor_1", 5000L, 50),
                        new WaterSensor("sensor_2", 6000L, 60));
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //方式一  将未注册的表注册为临时视图
        /*Table table = tableEnv.fromDataStream(waterSensorStream);
        tableEnv.createTemporaryView("sensor",table);*/
        //方式二  直接从流 转换为表 并注册表名
        tableEnv.createTemporaryView("sensor",waterSensorStream);
        //使用sql查询未注册的表
        Table table1 = tableEnv.sqlQuery("select * from " + "sensor" + " where id = 'sensor_1'");

        table1.execute().print();


    }
}
