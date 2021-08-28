package com.github.flink.study.watermark;

import com.github.flink.study.common.UserCountAgg;
import com.github.flink.study.common.UserEvent;
import com.github.flink.study.source.KafkaSourceDemo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * watermark 的作用是: 触发窗口计算
 */
public class WatermarkWithAllowedLatenessDemo
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<UserEvent> kafkaSource = new KafkaSourceDemo().createKafkaSource(env);

        kafkaSource
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(3L)))
                .allowedLateness(Time.seconds(2L))
                .aggregate(new UserCountAgg())
                .print("result==>");

        env.execute("kafka source demo");
    }
}
