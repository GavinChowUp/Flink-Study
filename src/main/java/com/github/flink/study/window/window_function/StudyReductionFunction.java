package com.github.flink.study.window.window_function;

import com.github.flink.study.common.FakeSource;
import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

//获取一段时间内(Window Size)每个用户(KeyBy)浏览的商品的最大价值的那条记录(UserEvent)
// 入：UserEvent,出UserEvent
@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class StudyReductionFunction
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = env
                .addSource(new FakeSource())
                .name("reduce-test-source");

        source.keyBy(UserEvent::getUserID)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(
                        (ReduceFunction<UserEvent>) (value1, value2) -> value1.getProductPrice() > value2.getProductPrice() ? value1 : value2
                )
                .print();
        env.execute("reduce-test");
    }
}
