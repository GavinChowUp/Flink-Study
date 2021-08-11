package com.github.flink.study.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.time.LocalDateTime;
import java.util.Random;

public class FakeSource
        implements SourceFunction<UserEvent>
{
    private static final Random random = new Random();
    private volatile boolean isRunning = true;

    private long sleepMillis;

    public FakeSource()
    {
        new FakeSource(500L);
    }

    public FakeSource(long sleepMills)
    {
        this.sleepMillis = sleepMills;
    }

    @Override
    public void run(SourceContext<UserEvent> ctx)
    {
        while (isRunning) {
            ctx.collect(UserEvent.builder()
                    .userID(random.nextInt(10))
                    .eventTime(LocalDateTime.now())
                    .eventType("browser")
                    .productID(random.nextInt(20))
                    .productPrice(Math.random())
                    .build());
        }
    }

    @Override
    public void cancel()
    {
        this.isRunning = false;
    }
}
