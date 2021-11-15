package com.github.flink.study.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.time.Duration;
import java.util.Properties;

public class KafkaSourceUtil
{
    public static SingleOutputStreamOperator<UserEvent> createKafkaSource(StreamExecutionEnvironment env, String topic, String groupId)
    {
        ObjectMapper objectMapper = new ObjectMapper();
        // 1.0 配置KafkaConsumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", groupId);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 1.1 把kafka设置为source
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props));

        return stream
                .map((MapFunction<String, UserEvent>) value -> objectMapper.readValue(value, UserEvent.class))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> {
                            String currentWatermark = new DateTime((element.getEventTime() - 1), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");
                            System.out.println("ThreadId: " + Thread.currentThread().getId()
                                    + "该条记录所携带的 watermark: " + currentWatermark);
                            return element.getEventTime();
                        }).withIdleness(Duration.ofSeconds(10L)));
    }
}
