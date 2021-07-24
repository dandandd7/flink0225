package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author yhm
 * @create 2021-07-20 15:17
 */
public class Flink09_CEP_CP_loop_greedy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor1.txt");
        SingleOutputStreamOperator<WaterSensor> watermarks = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.parseInt(split[2]));
            }
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (element, recordTimestamp) -> element.getTs() * 1000));
        Pattern<WaterSensor, WaterSensor> pattern = Pattern.<WaterSensor>begin("start")
                //迭代条件
//                .where(new IterativeCondition<WaterSensor>() {
//                    @Override
//                    public boolean filter(WaterSensor value, Context<WaterSensor> ctx) throws Exception {
//                        return "sensor_1".equals(value.getId());
//                    }
//                });
                //组合条件
        //简单条件
        .where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return "sensor_1".equals(value.getId());
            }
        })
                .times(1,3)
                .greedy()
                .next("end")
            .where(new SimpleCondition<WaterSensor>() {
                @Override
                public boolean filter(WaterSensor value) throws Exception {
                    return value.getVc()==30;
                }
            })

                ;
        PatternStream<WaterSensor> pattern1 = CEP.pattern(watermarks, pattern);
        pattern1.select(new PatternSelectFunction<WaterSensor, String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();
        env.execute();

    }
}
