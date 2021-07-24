package com.atguigu.day06;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @author yhm
 * @create 2021-07-20 11:25
 */
public class Flink08_statebackend {
    public static void main(String[] args) throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //基于内存的
        env.setStateBackend(new MemoryStateBackend());
        //基于文件系统的
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/flink/ck"));
        //基于Rocksdb
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/flink/Rocksdb"));
    }
}
