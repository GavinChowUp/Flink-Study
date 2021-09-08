package com.github.flink.study.watermark;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.source.WatermarkDemoSource;
import com.github.flink.study.watermark.agg_fun.UserEventCount;
import com.github.flink.study.watermark.agg_fun.UserEventCountAgg;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.TimeZone;

public class WatermarkDemo4
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UserEvent> source = env
                .addSource(new WatermarkDemoSource())
                .name("watermark-test");

        SingleOutputStreamOperator<UserEventCount> aggregate = source
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3L))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> element.getEventTime()))
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3L)))
                .aggregate(new UserEventCountAgg(), new WindowFunction<UserEventCount, UserEventCount, String, TimeWindow>()
                {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<UserEventCount> input, Collector<UserEventCount> out)
                    {
                        UserEventCount next = input.iterator().next();
                        System.out.println("level-1 window:==>" + window);
                        out.collect(next);
                    }
                });

        aggregate.print("level-1====>");

        aggregate
                .keyBy(UserEventCount::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .process(new ProcessWindowFunction<UserEventCount, Object, String, TimeWindow>()
                {
                    @Override
                    public void process(String s, ProcessWindowFunction<UserEventCount, Object, String, TimeWindow>.Context context, Iterable<UserEventCount> elements, Collector<Object> out)
                    {
                        System.out.println("level-2 watermark:==>" + LocalDateTime.ofInstant(Instant.ofEpochMilli(context.currentWatermark()), TimeZone.getDefault().toZoneId()));
                        System.out.println("level-2 window:==>" + context.window());
                    }
                }).print("level-2=====>");

        env.execute("kafka source demo");
    }
}
