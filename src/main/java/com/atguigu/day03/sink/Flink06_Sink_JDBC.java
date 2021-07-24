package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yhm
 * @create 2021-07-14 18:51
 */
public class Flink06_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9998);
        //有界流
//        final DataStreamSource<String> dataStreamSource = env.readTextFile("input/sensorName");
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                final String[] split = value.split(",");

                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        final DataStreamSink<WaterSensor> waterSensorDataStreamSink = map.addSink(JdbcSink.sink("insert into sensor values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {

                        preparedStatement.setString(1, waterSensor.getId());
                        preparedStatement.setLong(2, waterSensor.getTs());
                        preparedStatement.setInt(3, waterSensor.getVc());
                        System.out.println("数据写入。。。");
                    }
                },
                new JdbcExecutionOptions.Builder().withBatchIntervalMs(1000).build()
                , new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUsername("root").withPassword("123456")
                        .withUrl("jdbc:mysql://hadoop102:3306/test?useSSL=false").withDriverName("com.mysql.jdbc.Driver").build()));


        env.execute();
    }


}
