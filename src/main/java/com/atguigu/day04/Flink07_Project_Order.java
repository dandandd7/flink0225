package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @author yhm
 * @create 2021-07-16 19:25
 */
public class Flink07_Project_Order {
    //双流join
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<OrderEvent> order = env.readTextFile("input/OrderLog.csv").map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.valueOf(split[0]),
                        split[1],
                        split[2],
                        Long.valueOf(split[3]));
            }
        });
        SingleOutputStreamOperator<TxEvent> tx = env.readTextFile("input/ReceiptLog.csv").map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(
                        split[0],
                        split[1],
                        Long.valueOf(split[2]));
            }
        });
        ConnectedStreams<OrderEvent, TxEvent> connect = order.connect(tx);
        ConnectedStreams<OrderEvent, TxEvent> orderEventTxEventConnectedStreams = connect.keyBy("txId", "txId");
        orderEventTxEventConnectedStreams.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            HashMap<String, OrderEvent> stringOrderHashMap = new HashMap<>();
            HashMap<String, TxEvent> stringTxHashMap = new HashMap<>();
            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                if(stringTxHashMap.containsKey(value.getTxId())){
                    System.out.println("订单编号："+value.getOrderId()+"配对完成");
                    stringTxHashMap.remove(value.getTxId());
                }
                //为配对完成则加入缓存
                stringOrderHashMap.put(value.getTxId(),value);
            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                if(stringOrderHashMap.containsKey(value.getTxId())){
                    System.out.println("订单编号："+stringOrderHashMap.get(value.getTxId()).getOrderId()+"配对完成");
                    stringOrderHashMap.remove(value.getTxId());
                }
                //为配对完成则加入缓存
                stringTxHashMap.put(value.getTxId(),value);
            }
        }).print();
        env.execute();
    }
}
