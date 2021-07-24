package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @author yhm
 * @create 2021-07-23 14:33
 */
public class Flink07_UDF_TablerFun {
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
//        table.joinLateral(call(myTableFun.class,$("id")))
//                .select($("id"),$("ts"),$("vc"),$("word"))
//                .execute()
//                .print();
        //todo 4。注册后再使用
        tableEnv.createTemporarySystemFunction("mytablefunc",myTableFun.class);
//        table.joinLateral(call("mytablefunc",$("id")))
//                .select($("id"),$("word")).execute().print();
        //sql 写法
        tableEnv.executeSql("select id,word from "+table+" ,Lateral table (mytablefunc(id)) ").print();
    }
@FunctionHint(output = @DataTypeHint("Row<word String>"))
    public static class myTableFun extends TableFunction<Row> {
        public void eval(String value){
            String[] split = value.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }
    }
}
