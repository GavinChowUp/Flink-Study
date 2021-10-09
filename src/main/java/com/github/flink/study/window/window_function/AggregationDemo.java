package com.github.flink.study.window.window_function;

import com.github.flink.study.common.FakeSource;
import com.github.flink.study.common.UserEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import scala.Tuple2;

//获取某个用户(keyBy)在窗口时间浏览的商品价格平均值(入：UserEvent,出：double)
@Slf4j
public class AggregationDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = env
                .addSource(new FakeSource())
                .name("fake-test-source");

        source.keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AverageAggregateFunction())
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
}
