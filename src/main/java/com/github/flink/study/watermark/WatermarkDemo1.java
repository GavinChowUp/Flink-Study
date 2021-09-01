package com.github.flink.study.watermark;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.source.WatermarkDemoSource;
import com.github.flink.study.window.window_function.ReductionFunctionDemo;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WatermarkDemo1
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UserEvent> source = env
                .addSource(new WatermarkDemoSource())
                .name("watermark-test");
        source.print("input==>");

        SingleOutputStreamOperator<UserEvent> reduce = source
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3L)))
                .reduce(new ReductionFunctionDemo.MaxReduceFunction());

        reduce.print("level-1====>");

        reduce
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(10L)))
                .reduce((new ReductionFunctionDemo.MaxReduceFunction()))
                .print("level-2****>");

        env.execute("kafka source demo");
    }
}
