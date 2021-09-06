package com.alibaba.realtimecompute;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Properties;

public class DataStreamJobKafka2Rds{

    private static final String KAFKA_TOPIC = "kafka-order";

    //需要根据实际情况修改对应的参数
    private static final String KAFKA_BOOT_SERVERS =
            "192.168.XXX.XXX:9092,192.168.XXX.XXX:9092,192.168.XXX.XXX:9092";
    private static final String KAFKA_GROUP_ID = "demo-group2";

    private static final String INSERT_TEMPLATE =
            "INSERT INTO `%s` (`window_start`, `window_end`, `order_type`, `order_number`, `order_value_sum`) VALUES (?,?,?,?,?)";

    //需要根据实际情况修改对应的参数
    private static final String RDS_URL =
            "jdbc:mysql://XXXX:3306/test_db";
    private static final String RDS_USER_NAME = "XXXX";
    private static final String RDS_PASSWORD = "XXXXX";
    //根据不同的场景修改
    private static final String RDS_TABLE = "rds_new_table2";

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        String startTime = params.get("startTimestamp", null);
        long startTimestamp = System.currentTimeMillis();
        if (startTime != null) {
            startTimestamp = Timestamp.valueOf(startTime).getTime();
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BOOT_SERVERS);
        properties.setProperty("group.id", KAFKA_GROUP_ID);

        final TypeInformation<Row> SCHEMA =
                Types.ROW(
                        new String[] {"order_id", "order_time", "order_type", "order_value"},
                        new TypeInformation[] {
                                Types.STRING(), Types.SQL_TIMESTAMP(), Types.STRING(), Types.FLOAT()
                        });

        final CsvRowDeserializationSchema csvSchema =
                new CsvRowDeserializationSchema.Builder(SCHEMA).build();

        env.addSource(
                new FlinkKafkaConsumer<>(KAFKA_TOPIC, csvSchema, properties)
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Row>forBoundedOutOfOrderness(
                                        Duration.ofSeconds(2))
                                        .withTimestampAssigner(
                                                (event, timestamp) ->
                                                        ((Timestamp) event.getField(1))
                                                                .getTime()))
                        .setStartFromTimestamp(startTimestamp))
                .keyBy(row -> (String) row.getField(2))
                .timeWindow(Time.minutes(5))
                .process(
                        new ProcessWindowFunction<Row, OutputData, String, TimeWindow>() {
                            @Override
                            public void process(
                                    String key,
                                    Context context,
                                    Iterable<Row> iterable,
                                    Collector<OutputData> collector)
                                    throws Exception {
                                long orderNumber = 0;
                                double orderValueSum = 0;
                                for (Row row : iterable) {
                                    orderNumber++;
                                    orderValueSum += ((Float) row.getField(3)).doubleValue();
                                }
                                collector.collect(
                                        new OutputData(
                                                new Timestamp(context.window().getStart()),
                                                new Timestamp(context.window().getEnd()),
                                                key,
                                                orderNumber,
                                                orderValueSum));
                            }
                        })
                .addSink(
                        JdbcSink.<OutputData>sink(
                                String.format(INSERT_TEMPLATE, RDS_TABLE),
                                (ps, outputData) -> {
                                    ps.setTimestamp(1, outputData.windowStart);
                                    ps.setTimestamp(2, outputData.windowEnd);
                                    ps.setString(3, outputData.orderType);
                                    ps.setLong(4, outputData.orderNumber);
                                    ps.setDouble(5, outputData.orderValueSum);
                                },
                                new JdbcExecutionOptions.Builder()
                                        .withBatchIntervalMs(3000)
                                        .withBatchSize(100)
                                        .withMaxRetries(3)
                                        .build(),
                                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                        .withUrl(RDS_URL)
                                        .withUsername(RDS_USER_NAME)
                                        .withPassword(RDS_PASSWORD)
                                        .withDriverName("com.mysql.jdbc.Driver")
                                        .build()));
        env.execute("kafka-to-rds");
    }

    private static class OutputData {
        public Timestamp windowStart;
        public Timestamp windowEnd;
        public String orderType;
        public long orderNumber;
        public double orderValueSum;

        public OutputData(
                Timestamp windowStart,
                Timestamp windowEnd,
                String orderType,
                long orderNumber,
                double orderValueSum) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.orderType = orderType;
            this.orderNumber = orderNumber;
            this.orderValueSum = orderValueSum;
        }
    }
}