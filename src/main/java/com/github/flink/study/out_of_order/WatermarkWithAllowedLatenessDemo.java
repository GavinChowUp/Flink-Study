package com.github.flink.study.out_of_order;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.source.OutOfOrderSourceDemo;
import com.github.flink.study.watermark.agg_fun.UserEventCount;
import com.github.flink.study.watermark.agg_fun.UserEventCountAgg;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

public class WatermarkWithAllowedLatenessDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UserEvent> kafkaSource = new OutOfOrderSourceDemo().createKafkaSource(env);

        SingleOutputStreamOperator<UserEventCount> aggregate = kafkaSource
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3L)))
                .sideOutputLateData(new OutputTag<UserEvent>("Late Record") {})
                .aggregate(new UserEventCountAgg());

        aggregate.print("result==>");
        aggregate.getSideOutput(new OutputTag<UserEvent>("Late Record") {}).print("Late record==>");

        env.execute("kafka source demo");
    }
}
