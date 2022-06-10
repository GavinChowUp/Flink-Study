package com.github.flink.study.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.flink.study.common.UserEvent;
import com.github.flink.study.util.EventBuilderUtil;

public class MultipleThreadKafkaMessageSender
{
    static ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args)
            throws JsonProcessingException, InterruptedException
    {
        KafkaMessageProducer producer = new KafkaMessageProducer("topic_all");

        while (true) {
            UserEvent userEvent = EventBuilderUtil.BuildUserEvent();
            producer.send(objectMapper.writeValueAsString(userEvent));
        }
    }
}
