package com.github.flink.study.parallelism;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.kafka.KafkaSourceUtil;
import com.github.flink.study.window.window_function.ReductionFunctionDemo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class KafkaEqualFlink
{
    public static void main(String[] args)
            throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(4);

        SingleOutputStreamOperator<UserEvent> kafkaSource = KafkaSourceUtil.createKafkaSource(env, "watermark_source", "kafka_equal_flink");

        kafkaSource.print(" source input ==>");
        kafkaSource
                .keyBy(UserEvent::getUserId)
                .window(TumblingEventTimeWindows.of(Time.seconds(60L)))
                .reduce(new ReductionFunctionDemo.MaxReduceFunction(), new ReductionFunctionDemo.MaxProcessWindowFunction())
                .print(" result ==>");
        env.execute("kafka_equal_flink");
    }
}
