package com.atguigu.day04;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;


import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @author yhm
 * @create 2021-07-16 18:39
 */
public class Flink04_Project_AppAnalysis_By_Chanel {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //分渠道
        DataStreamSource<MarketingUserBehavior> dataStreamSource = env.addSource(new AppMarketingDataSource());
        dataStreamSource.map(new MapFunction<MarketingUserBehavior, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(MarketingUserBehavior value) throws Exception {
                return new Tuple2<>(value.getChannel()+"-"+value.getBehavior(),1L);
            }
        }).keyBy(0).sum(1).print();


        env.execute();
    }
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }

}
