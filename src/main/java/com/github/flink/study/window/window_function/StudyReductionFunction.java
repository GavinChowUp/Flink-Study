package com.github.flink.study.window.window_function;

import com.github.flink.study.common.FakeSource;
import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
public class StudyReductionFunction
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = env
                .addSource(new FakeSource())
                .name("reduce-test");

        source.keyBy(UserEvent::getUserID)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((ReduceFunction<UserEvent>) (value1, value2) -> value1.getProductPrice() > value2.getProductPrice() ? value1 : value2).print();
        env.execute("test-source");
    }
}
