package com.github.flink.study.source;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.util.EventBuilderUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class LocalSingleUserEventSource
        implements SourceFunction<UserEvent>
{
    private volatile boolean isRunning = true;

    private long sleepMillis;

    public LocalSingleUserEventSource()
    {
        this(5000L);
    }

    private LocalSingleUserEventSource(long sleepMills)
    {
        this.sleepMillis = sleepMills;
    }

    @Override
    public void run(SourceContext<UserEvent> ctx)
            throws InterruptedException
    {
        while (isRunning) {
            EventBuilderUtil.BuildUserEvent();
            Thread.sleep(sleepMillis);
        }
    }

    @Override
    public void cancel()
    {
        this.isRunning = false;
    }
}
