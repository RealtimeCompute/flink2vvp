package com.alibaba.realtimecompute;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class TableJobKafka2Rds {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnvironment = TableEnvironment.create(settings);

        //需要根据实际情况修改对应的参数
        String kafkaDDL = "CREATE TEMPORARY TABLE kafkatable (\n" +
                "                order_id varchar,\n" +
                "                order_time timestamp(3),\n" +
                "                order_type varchar,\n" +
                "                order_value float,\n" +
                "                WATERMARK FOR order_time AS order_time - INTERVAL '2' SECOND\n" +
                "                )\n" +
                "                WITH ( 'connector'='kafka',\n" +
                "                'topic' = 'kafka-order',\n" +
                "                'properties.bootstrap.servers' = '192.168.XXX.XXX:9092,192.168.XXX.XXX:9092,192.168.XXX.XXX:9092',\n" +
                "                'properties.group.id' = 'demo-group',\n" +
                "                'scan.startup.mode' = 'latest-offset',\n" +
                "                'format' = 'csv')";
        ////根据不同的场景修改table-name
        String rdsDDL = "CREATE TEMPORARY TABLE rdstable (\n" +
                "                window_start timestamp,\n" +
                "                window_end timestamp,\n" +
                "                order_type varchar,\n" +
                "                order_number bigint,\n" +
                "                order_value_sum double,\n" +
                "                PRIMARY KEY (window_start,window_end,order_type) NOT ENFORCED\n" +
                "                ) WITH (\n" +
                "                'connector' = 'jdbc',\n" +
                "                'url' = 'jdbc:mysql://XXXX:3306/test_db',\n" +
                "                'table-name' = 'rds_new_table3',\n" +
                "                'driver' = 'com.mysql.jdbc.Driver',\n" +
                "                'username' = 'XXXX',\n" +
                "                'password' = 'XXXX'\n" +
                "                )";

        tableEnvironment.executeSql(kafkaDDL);

//        写出到rds
        tableEnvironment.executeSql(rdsDDL);
        tableEnvironment.sqlQuery("SELECT\n" +
                "    TUMBLE_START(order_time, INTERVAL '5' MINUTE) as window_start,\n" +
                "    TUMBLE_END(order_time, INTERVAL '5' MINUTE) as window_end,\n" +
                "    order_type,\n" +
                "    COUNT(1) as order_number,\n" +
                "    SUM(order_value) as order_value_sum\n" +
                "    FROM kafkatable\n" +
                "    GROUP BY TUMBLE(order_time, INTERVAL '5' MINUTE),order_type")
                .executeInsert("rdstable");
    }
}
