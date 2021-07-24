package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.hadoop.util.Waitable;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author yhm
 * @create 2021-07-23 14:33
 */
public class Flink06_UDF_ScalarFun {
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
//        table.select($("id"),call(myScalarFun.class,$("id"))).execute().print();
        //todo 4. 先注册再使用
        tableEnv.createTemporarySystemFunction("myscalarfunc",myScalarFun.class);
//        table.select($("id"),call("myscalarfunc",$("id"))).execute().print();
        //todo 4. sql 写法
        tableEnv.executeSql("select id,myscalarfunc(id) from "+table).print();

    }


    public static class myScalarFun extends ScalarFunction{
        public int eval(String value){
            return value.length();
        }
    }
}
