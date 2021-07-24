package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yhm
 * @create 2021-07-19 16:37
 */
public class processingtime {
    public static void main(String[] args) throws Exception {
        //监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据并转换为JavaBean
//3.按照传感器ID分组
        //4.使用ProcessFunction实现5秒种水位不下降，则报警，且将报警信息输出到侧输出流
        env.execute();
    }
}
