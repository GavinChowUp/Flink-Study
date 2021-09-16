package com.github.flink.study.out_of_order;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.window.window_function.ReductionFunctionDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

//只有窗口，没有设置watermark， flink默认抛出异常
public class OnlyWindowDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        ObjectMapper objectMapper = new ObjectMapper();
        SingleOutputStreamOperator<UserEvent> source = env.socketTextStream("localhost", 9999)
                .map(event -> objectMapper.readValue(event, UserEvent.class)).setParallelism(1);
        source.print("====>source");
        source
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .reduce(new ReductionFunctionDemo.MaxReduceFunction()).print("result====>");

        env.execute("Processing window Job");
    }
}
