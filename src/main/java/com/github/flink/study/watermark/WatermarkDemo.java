package com.github.flink.study.watermark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * watermark 的作用是: 触发窗口计算
 * 场景1：
 */
public class WatermarkDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper objectMapper = new ObjectMapper();
        env.socketTextStream("localhost", 9999)
                .map(event -> objectMapper.readValue(event, UserEvent.class))
                .keyBy(UserEvent::getUserId)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<UserEvent>()
                {
                    @Override
                    public long extractTimestamp(UserEvent element, long recordTimestamp)
                    {
                        return 0;
                    }
                }));

        env.execute("Processing window Job");
    }

    public static class CountReduceFunction
            implements ReduceFunction<UserEvent>
    {
        @Override
        public UserEvent reduce(UserEvent value1, UserEvent value2)
        {
            value1.setProductPrice(value1.getProductPrice() + value2.getProductPrice());
            return value1;
        }
    }
}
