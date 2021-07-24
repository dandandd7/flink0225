package com.atguigu.day04;

import com.atguigu.bean.AdsClickLog;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-16 18:58
 */
public class Flink06_Project_Ads_Click {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.readTextFile("input/AdClickLog.csv")
                .map(new MapFunction<String, AdsClickLog>() {
                    @Override
                    public AdsClickLog map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new AdsClickLog(Long.valueOf(split[0]),
                                Long.valueOf(split[1]),
                                split[2],
                                split[3],
                                Long.valueOf(split[4]));
                    }
                })
                .map(new MapFunction<AdsClickLog, Tuple2<String,Long>>() {
                    @Override
                    public Tuple2<String, Long> map(AdsClickLog value) throws Exception {
                        return Tuple2.of(value.getProvince()+"-"+value.getAdId(),1L);
                    }
                })
                .keyBy(0).sum(1).print();
        env.execute();
    }
}
