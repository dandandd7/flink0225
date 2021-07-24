package com.atguigu.day09;


import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author yhm
 * @create 2021-07-23 14:33
 */
public class Flink09_UDF_TableAggFun {
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
//        table.groupBy($("id"))
//                .flatAggregate(call(MytableaggFun.class,$("vc")).as("value","level"))
//                .select($("id"),$("value"),$("level")).execute().print();
        //todo 4。注册后再使用
        tableEnv.createTemporarySystemFunction("mytaaggfunc",MytableaggFun.class);
        table.groupBy($("id"))
                .flatAggregate(call("mytaaggfunc",$("vc")).as("value","level"))
                .select($("id"),$("value"),$("level")).execute().print();
        //无sql 写法

    }



    public static class MyTableAccumulator{
        public int first = Integer.MIN_VALUE;
        public int second = Integer.MIN_VALUE;
    }

    public static class MytableaggFun extends TableAggregateFunction<Tuple2<Integer,String>,MyTableAccumulator> {
        @Override
        public MyTableAccumulator createAccumulator() {
            return new MyTableAccumulator();
        }
        public void accumulate(MyTableAccumulator acc,Integer value){
            if(value>acc.first){
                acc.second = acc.first;
                acc.first = value;
            }else if(value>acc.second){
                acc.second=value;
            }
        }
        public void emitValue(MyTableAccumulator acc, Collector<Tuple2<Integer,String>> out){
            if(acc.first!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.first,"1st.max"));
            }if(acc.second!=Integer.MIN_VALUE){
                out.collect(Tuple2.of(acc.second,"2nd.max"));
            }
        }
    }
}
