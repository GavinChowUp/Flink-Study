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
        this(500L);
    }

    private FakeSource(long sleepMills)
    {
        this.sleepMillis = sleepMills;
    }

    @Override
    public void run(SourceContext<UserEvent> ctx)
            throws InterruptedException
    {
        while (isRunning) {
            ctx.collect(UserEvent.builder()
                    .userID(random.nextInt(10))
                    .eventTime(LocalDateTime.now())
                    .eventType("browser")
                    .productID(random.nextInt(20))
                    .productPrice(Math.random() * 100)
                    .build());
            Thread.sleep(sleepMillis);
        }
    }

    @Override
    public void cancel()
    {
        this.isRunning = false;
    }
}
