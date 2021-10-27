package com.github.flink.study.topn;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.source.LocalMultipleUserEventsSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

import java.time.Duration;
import java.util.TreeSet;

public class TopNDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = env
                .addSource(new LocalMultipleUserEventsSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((SerializableTimestampAssigner<UserEvent>) (element, recordTimestamp) -> element.getEventTime()).withIdleness(Duration.ofSeconds(10L)))
                .name("fake-test-source");

        source.keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new PartialTopN())
                .print();

        env.execute("Aggregate Job");
    }

    private static class PartialTopN
            implements AggregateFunction<UserEvent, TreeSet<Tuple2<String, Double>>, TreeSet<Tuple2<String, Double>>>
    {
        @Override
        public TreeSet<Tuple2<String, Double>> createAccumulator()
        {
            return new TreeSet<>((o1, o2) -> o2._2.compareTo(o1._2));
        }

        @Override
        public TreeSet<Tuple2<String, Double>> add(UserEvent value, TreeSet<Tuple2<String, Double>> accumulator)
        {
            accumulator.add(new Tuple2<>(value.getUserId(), value.getProductPrice()));
            if (accumulator.size() > 10) {
                accumulator.pollLast();
            }
            return accumulator;
        }

        @Override
        public TreeSet<Tuple2<String, Double>> getResult(TreeSet<Tuple2<String, Double>> accumulator)
        {
            System.out.println("当前size:" + accumulator.size());
            return accumulator;
        }

        @Override
        public TreeSet<Tuple2<String, Double>> merge(TreeSet<Tuple2<String, Double>> a, TreeSet<Tuple2<String, Double>> b)
        {
            a.addAll(b);
            return a;
        }
    }
}
