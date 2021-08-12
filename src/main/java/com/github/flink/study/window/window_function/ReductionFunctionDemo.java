package com.github.flink.study.window.window_function;

import com.github.flink.study.common.FakeSource;
import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

//获取一段时间内(Window Size)每个用户(KeyBy)浏览的商品的最大价值的那条记录(UserEvent)
//获取一段时间内(Window Size)每个用户(KeyBy)浏览的商品的最大价值的那条记录(UserEvent), 并获得所在窗口
// 入：UserEvent,出UserEvent;
public class ReductionFunctionDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = env
                .addSource(new FakeSource())
                .name("reduce-test-source");

        source.keyBy(UserEvent::getUserId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new MaxReduceFunction(), new MaxProcessWindowFunction())
                .print();
        env.execute("reduce-test");
    }

    public static class MaxReduceFunction
            implements ReduceFunction<UserEvent>
    {
        @Override
        public UserEvent reduce(UserEvent value1, UserEvent value2)
        {
            value1.setProductPrice(value1.getProductPrice() + value2.getProductPrice());
            return value1;
        }
    }

    public static class MaxProcessWindowFunction
            extends ProcessWindowFunction<UserEvent, String, String, TimeWindow>
    {
        @Override
        public void process(String key, Context context, Iterable<UserEvent> elements, Collector<String> out)
        {
            String windowStart = new DateTime(context.window().getStart(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");
            String windowEnd = new DateTime(context.window().getEnd(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");

            String record = "Key: " + key + " 窗口开始时间: " + windowStart + " 窗口结束时间: "
                    + windowEnd + " 浏览的商品的最大价值对应的那条记录: " + elements.iterator().next();
            out.collect(record);
        }
    }
}
