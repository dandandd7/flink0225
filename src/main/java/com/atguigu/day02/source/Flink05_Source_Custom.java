package com.atguigu.day02.source;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.kafka.common.protocol.types.Field;

import javax.swing.tree.VariableHeightLayoutCache;
import java.util.Random;

/**
 * @author yhm
 * @create 2021-07-13 20:33
 */
public class Flink05_Source_Custom {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final DataStreamSource<WaterSensor> waterSensorDataStreamSource = env.addSource(new MySource());
        waterSensorDataStreamSource.print();
        env.execute();
    }

    public static class MySource implements SourceFunction<WaterSensor> {
        private Random random = new Random();
        Boolean flag = true;
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            while (flag) {
                ctx.collect(new WaterSensor("sensor" +random.nextInt(10),System.currentTimeMillis(),random.nextInt(10)*100));
                Thread.sleep(1000);
            }
        }

        public void cancel() {
            flag=false;
        }
    }
}



