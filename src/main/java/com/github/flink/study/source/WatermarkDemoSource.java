package com.github.flink.study.source;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.util.EventBuilder;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class WatermarkDemoSource
        implements SourceFunction<UserEvent>
{
    private volatile boolean isRunning = true;

    private long sleepMillis;

    public WatermarkDemoSource()
    {
        this(5000L);
    }

    private WatermarkDemoSource(long sleepMills)
    {
        this.sleepMillis = sleepMills;
    }

    @Override
    public void run(SourceContext<UserEvent> ctx)
            throws InterruptedException
    {
        while (isRunning) {
            EventBuilder.BuildUserEvent();
            Thread.sleep(sleepMillis);
        }
    }

    @Override
    public void cancel()
    {
        this.isRunning = false;
    }
}
