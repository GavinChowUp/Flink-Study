package com.github.flink.study.out_of_order;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.watermark.agg_fun.UserEventCountAgg;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//只有窗口，没有设置watermark， flink默认抛出异常
public class OnlyWindowDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper objectMapper = new ObjectMapper();
        env.socketTextStream("localhost", 9999)
                .map(event -> objectMapper.readValue(event, UserEvent.class))
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .aggregate(new UserEventCountAgg());

        env.execute("Processing window Job");
    }
}
