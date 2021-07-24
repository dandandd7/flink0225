package com.atguigu.day09;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @author yhm
 * @create 2021-07-23 17:13
 */
public class Flink10_HiveCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置用户权限
        System.setProperty("HADOOP_USER_NAME","atguigu");
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //连接Hive
        String catalogName = "hivecata";
        String datebaseName = "company";
        String hiveConfDir = "input/hive-conf";
        // 1. 创建HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(catalogName,datebaseName,hiveConfDir);
        // 2. 注册HiveCatalog
        tableEnv.registerCatalog(catalogName,hiveCatalog);
        // 3. 把 HiveCatalog: myhive 作为当前session的catalog
        tableEnv.useCatalog(catalogName);
        tableEnv.useDatabase(datebaseName);

        //指定SQL语法为Hive语法
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("select * from stu").print();
    }
}
