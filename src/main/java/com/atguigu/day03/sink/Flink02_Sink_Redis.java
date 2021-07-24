package com.atguigu.day03.sink;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author yhm
 * @create 2021-07-14 18:51
 */
public class Flink02_Sink_Redis {
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
        final FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("hadoop102").setPort(6379).build();
        map.addSink(new RedisSink<WaterSensor>(config,new MySinkRedis()));

        env.execute();
    }
    public static class MySinkRedis implements RedisMapper<WaterSensor>{

        @Override
        /**
         * 控制
         */
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        /**
         * key
         */
        public String getKeyFromData(WaterSensor data) {
            return data.getId();
        }

        @Override
        /**
         * Extracts value from data.
         */
        public String getValueFromData(WaterSensor data) {
            return JSONObject.toJSONString(data);
        }
    }
}
