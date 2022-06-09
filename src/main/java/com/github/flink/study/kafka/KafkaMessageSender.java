package com.github.flink.study.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.flink.study.common.UserEvent;
import com.github.flink.study.util.EventBuilderUtil;

public class KafkaMessageSender
{
    public static void main(String[] args)
            throws JsonProcessingException, InterruptedException
    {
        KafkaMessageProducer producer = new KafkaMessageProducer("roubin_partition_strategy");

        ObjectMapper objectMapper = new ObjectMapper();

        while (true) {
            UserEvent userEvent = EventBuilderUtil.BuildUserEvent();
            producer.send(objectMapper.writeValueAsString(userEvent));
            Thread.sleep(10);
        }
    }
}
