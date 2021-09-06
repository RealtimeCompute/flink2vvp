from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


def demo():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    t_env = StreamTableEnvironment.create(s_env,
                                          environment_settings=
                                          EnvironmentSettings.new_instance()
                                          .in_streaming_mode()
                                          .use_blink_planner()
                                          .build())
    # 添加依赖的conector jar,
    # TODO: 路径需要根据实际情况调整
    t_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///root/flink/opt/flink-connector-jdbc_2.11-1.11.4.jar;"
        "file:///root/flink/opt/mysql-connector-java-8.0.20.jar;"
        "file:///root/flink/opt/flink-sql-connector-kafka_2.11-1.11.4.jar")

    kafka_topic = 'kafka-order'
    boot_servers = '192.168.XXX.XXX:9092,192.168.XXX.XXX:9092,192.168.XXX.XXX:9092'
    group_id = 'demo-group'
    kafka_ddl = f"""
        CREATE TEMPORARY TABLE kafkatable (
            order_id varchar,
            order_time timestamp(3),
            order_type varchar,
            order_value float,
            WATERMARK FOR order_time AS order_time - INTERVAL '2' SECOND
        ) WITH (
            'connector'='kafka',
            'topic' = '{kafka_topic}',
            'properties.bootstrap.servers' = '{boot_servers}',
            'properties.group.id' = '{group_id}',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'csv')
        """

    url = 'jdbc:mysql://XXXX:3306/test_db'
    # TODO: table_name需要根据实际情况调整
    table_name = 'rds_old_table4'
    user_name = 'XXXX'
    password = 'XXXX'
    rds_ddl = f"""
        CREATE TEMPORARY TABLE rdstable (
            window_start timestamp,
            window_end timestamp,
            order_type varchar,
            order_number bigint,
            order_value_sum double,
            PRIMARY KEY (window_start,window_end,order_type) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{url}',
            'table-name' = '{table_name}',
            'driver' = 'com.mysql.jdbc.Driver',
            'username' = '{user_name}',
            'password' = '{password}'
        )
    """

    t_env.execute_sql(kafka_ddl)
    t_env.execute_sql(rds_ddl)
    t_env.sql_query("""SELECT
        TUMBLE_START(order_time, INTERVAL '5' MINUTE) as window_start,
        TUMBLE_END(order_time, INTERVAL '5' MINUTE) as window_end,
        order_type,
        COUNT(1) as order_number,
        SUM(order_value) as order_value_sum
        FROM kafkatable
        GROUP BY TUMBLE(order_time, INTERVAL '5' MINUTE), order_type
    """).execute_insert("rdstable")


if __name__ == '__main__':
    demo()
