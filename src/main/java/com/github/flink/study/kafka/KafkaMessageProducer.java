package com.github.flink.study.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaMessageProducer
{
    private final String topicName;
    private final KafkaProducer kafkaProducer;

    public KafkaMessageProducer(String topicName)
    {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("retries", 0);
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "org.apache.kafka.clients.producer.RoundRobinPartitioner");
        props.put("linger.ms", 200);
        props.put("batch.size", 16 * 1024 * 5);
        props.put("compression.type", "snappy");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        this.topicName = topicName;
        this.kafkaProducer = new KafkaProducer(props);
    }

    public void send(String message)
    {
        kafkaProducer.send(new ProducerRecord(topicName, message));
    }
}
