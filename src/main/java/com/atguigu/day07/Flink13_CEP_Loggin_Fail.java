package com.atguigu.day07;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author yhm
 * @create 2021-07-20 18:16
 */
public class Flink13_CEP_Loggin_Fail {
    public static void main(String[] args) throws Exception {
        ////用户2秒内连续两次及以上登录失败则判定为恶意登录。
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        KeyedStream<LoginEvent, Long> keyedStream = env.readTextFile("input/LoginLog.csv")
                .map(new MapFunction<String, LoginEvent>() {
                    @Override
                    public LoginEvent map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new LoginEvent(Long.valueOf(split[0]), split[1], split[2], Long.valueOf(split[3]) * 1000L);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(20)).withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime();
                    }
                }))
                .keyBy(LoginEvent::getUserId);
        Pattern<LoginEvent, LoginEvent> within = Pattern
                .<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                }).timesOrMore(2).within(Time.seconds(2));
        PatternStream<LoginEvent> pattern = CEP.pattern(keyedStream, within);
        pattern.select(new PatternSelectFunction<LoginEvent, String>() {

            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                return pattern.toString();
            }
        }).print();
        env.execute();
    }
}
