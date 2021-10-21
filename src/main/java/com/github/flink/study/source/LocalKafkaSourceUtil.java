package com.github.flink.study.source;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.common.UserEventDeserializeSchema;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class LocalKafkaSourceUtil
{
    public DataStream<UserEvent> createKafkaSourceWithWatermark(StreamExecutionEnvironment env)
    {
        KafkaSource<UserEvent> source = KafkaSource.<UserEvent>builder().setBootstrapServers("localhost:9092")
                .setTopics("watermark_source")
                .setGroupId("test")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new UserEventDeserializeSchema()).build();

        return env.fromSource(source,
                WatermarkStrategy
                        .<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> element.getEventTime()),
                "kafka source");
    }
}
