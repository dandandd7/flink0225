package com.atguigu.day06;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author yhm
 * @create 2021-07-19 21:57
 */
public class broadcastStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        DataStreamSource<String> dataStream = env.socketTextStream("hadoop102", 9998);
        DataStreamSource<String> controlStream = env.socketTextStream("hadoop102", 8888);
        MapStateDescriptor<String, String> boradcast1 = new MapStateDescriptor<>("boradcast", String.class, String.class);
        //广播一条流
        BroadcastStream<String> boradcast = controlStream.broadcast(boradcast1);
        BroadcastConnectedStream<String, String> connect = dataStream.connect(boradcast);
        //处理流
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 从广播状态中取值, 不同的值做不同的业务
                ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(boradcast1);
                if("1".equals(state.get("switch"))){
                    out.collect("切换到1号配置");
                }else {
                    out.collect("切换到2号配置");
                }
            }

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {
                //提取状态
                BroadcastState<String,String> broadcastState = ctx.getBroadcastState(boradcast1);

                //把值放入广播状态
                broadcastState.put("switch",value);
            }
        }).print();
        env.execute();

    }


}
