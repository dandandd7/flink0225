package com.atguigu.day09;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author yhm
 * @create 2021-07-23 14:33
 */
public class Flink08_UDF_AggFun {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> dataStreamSource = env.socketTextStream("hadoop102", 9998);
        SingleOutputStreamOperator<WaterSensor> stringStream = dataStreamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
            }
        });
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table table = tableEnv.fromDataStream(stringStream);
        //todo 4. 不注册函数直接使用TableAPI
//        table.groupBy($("id")).select($("id"),call(MyAvgFun.class,$("vc")).as("vcAvg")).execute().print();
        //todo 4。注册后再使用
        tableEnv.createTemporarySystemFunction("myavgfunc",MyAvgFun.class);
//        table.groupBy($("id"))
//                .select($("id"),call("myavgfunc",$("vc"))).execute().print();
        //sql 写法
        tableEnv.executeSql("select id,myavgfunc(vc) from "+table +" group by id").print();
    }



    public static class MyAccumulator{
        public long sum = 0;
        public int count = 0;
    }

    public static class MyAvgFun extends AggregateFunction<Double,MyAccumulator> {
        @Override
        public MyAccumulator createAccumulator() {
            return new MyAccumulator();
        }
        public void accumulate(MyAccumulator myAccumulator,Integer value){
            myAccumulator.count += 1;
            myAccumulator.sum += value;
        }
        @Override
        public Double getValue(MyAccumulator myAccumulator) {
            return myAccumulator.sum*1D/myAccumulator.count;
        }
    }
}
