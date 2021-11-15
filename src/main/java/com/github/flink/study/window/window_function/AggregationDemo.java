package com.github.flink.study.window.window_function;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.source.LocalMultipleUserEventsSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

import java.time.Duration;

//获取某个用户(keyBy)在窗口时间浏览的商品价格平均值(入：UserEvent,出：double)
@Slf4j
public class AggregationDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStream<UserEvent> source = env
                .addSource(new LocalMultipleUserEventsSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> element.getEventTime()).withIdleness(Duration.ofSeconds(10L)))
                .name("fake-test-source");

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new FsStateBackend("file:///Users/yanzhou/checkpoint"));

        source.keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AverageKryoAggregateFunction())
                .print();

        env.execute("Aggregate Job");
    }

    private static class AverageAggregateFunction
            implements AggregateFunction<UserEvent, Tuple2<Double, Long>, Double>
    {
        @Override
        public Tuple2<Double, Long> createAccumulator()
        {
            return new Tuple2<>(0d, 0L);
        }

        @Override
        public Tuple2<Double, Long> add(UserEvent value, Tuple2<Double, Long> accumulator)
        {
            return new Tuple2<>(accumulator._1 + value.getProductPrice(), accumulator._2 + 1);
        }

        @Override
        public Double getResult(Tuple2<Double, Long> accumulator)
        {
            return accumulator._1 / accumulator._2;
        }

        @Override
        public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b)
        {
            return new Tuple2<>(a._1 + b._1, a._2 + b._2);
        }
    }

    private static class AverageKryoAggregateFunction
            implements AggregateFunction<UserEvent, UserEvent, Double>
    {
        @Override
        public UserEvent createAccumulator()
        {
            return UserEvent.builder().build();
        }

        @Override
        public UserEvent add(UserEvent value, UserEvent accumulator)
        {
            return value;
        }

        @Override
        public Double getResult(UserEvent accumulator)
        {
            return 999D;
        }

        @Override
        public UserEvent merge(UserEvent a, UserEvent b)
        {
            return null;
        }
    }
}
