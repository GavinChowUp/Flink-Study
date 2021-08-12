package com.github.flink.study.window.keyby_selector;

import com.github.flink.study.common.FakeSource;
import com.github.flink.study.common.UserEvent;
import com.github.flink.study.window.window_function.StudyReductionFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StudyKeybySelector
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<UserEvent> source = env
                .addSource(new FakeSource())
                .name("fake-test-source");

        source.keyBy((KeySelector<UserEvent, String>) value -> value.getUserID() + "_" + value.getUserEventType())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new StudyReductionFunction.MaxReduceFunction(), new StudyReductionFunction.MaxProcessWindowFunction())
                .print();

        env.execute("Processing window Job");
    }
}
