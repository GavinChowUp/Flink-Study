package com.github.flink.study.window.window_function;

import com.github.flink.study.common.FakeSource;
import com.github.flink.study.common.UserEvent;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

//获取Window size内，每个用户浏览商品的总价值
public class ProcessWindowDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = env
                .addSource(new FakeSource())
                .name("fake-test-source");

        source.keyBy(UserEvent::getUserId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new SumProcessWindow())
                .print();

        env.execute("Processing window Job");
    }

    //获取Window size内，每个用户浏览商品的总价值
    public static class SumProcessWindow
            extends ProcessWindowFunction<UserEvent, String, String, TimeWindow>
    {
        @Override
        public void process(String key, ProcessWindowFunction<UserEvent, String, String, TimeWindow>.Context context, Iterable<UserEvent> elements, Collector<String> out)
        {
            double sum = 0d;
            int count = 0;
            for (UserEvent element : elements) {
                sum += element.getProductPrice();
                count++;
            }
            String windowStart = new DateTime(context.window().getStart(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");
            String windowEnd = new DateTime(context.window().getEnd(), DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss");

            String record = "Key: " + key + " 窗口开始时间: "
                    + windowStart + " 窗口结束时间: " + windowEnd + " 浏览的商品总价值: " + sum + " elements size:" + count;
            out.collect(record);
        }
    }
}
