package com.github.flink.study.watermark;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.source.WatermarkDemoSource;
import com.github.flink.study.window.window_function.ReductionFunctionDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WatermarkDemo6
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(10);
        DataStream<UserEvent> source = env
                .addSource(new WatermarkDemoSource())
                .name("watermark-test");

        System.out.println("=========================================");
        System.out.println(env.getConfig().getAutoWatermarkInterval());

        env.getConfig().setAutoWatermarkInterval(200L);

        source.print("source==>");

        source
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(2L)))
                .reduce(new ReductionFunctionDemo.MaxReduceFunction()).print("====>result");

        env.execute("kafka source demo");
    }
}
