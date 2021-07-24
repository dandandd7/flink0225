package com.atguigu.day03.sink;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author yhm
 * @create 2021-07-14 18:51
 */
public class Flink05_Sink_Custom_Rich {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9998);
        SingleOutputStreamOperator<WaterSensor> map = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                final String[] split = value.split(",");

                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        map.addSink(new MyJDBC());

        env.execute();
    }

    public static class MyJDBC extends RichSinkFunction<WaterSensor> {
        Connection connection ;
        PreparedStatement ps;
        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            ps = connection.prepareStatement("insert into sensor values (?,?,?)");
        }

        @Override
        public void close() throws Exception {
            ps.close();
            connection.close();
        }

        @Override
        public void invoke(WaterSensor value, Context context) throws Exception {
            System.out.println("invoking...");
            ps.setString(1,value.getId());
            ps.setLong(2,value.getTs());
            ps.setInt(3,value.getVc());
            ps.execute();
        }
    }

}
