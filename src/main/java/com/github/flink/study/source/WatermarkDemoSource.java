package com.github.flink.study.source;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.common.UserEventType;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.security.SecureRandom;
import java.util.Random;

public class WatermarkDemoSource
        implements SourceFunction<UserEvent>
{
    private static final Random SECURE_RANDOM = new SecureRandom();
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

    public static <T extends Enum<T>> T randomEnum(Class<T> clazz)
    {
        int x = SECURE_RANDOM.nextInt(clazz.getEnumConstants().length);
        return clazz.getEnumConstants()[x];
    }

    @Override
    public void run(SourceContext<UserEvent> ctx)
            throws InterruptedException
    {
        while (isRunning) {
            ctx.collect(UserEvent.builder()
                    .userId("user_" + 1)
                    .eventTime(System.currentTimeMillis())
                    .userEventType(randomEnum(UserEventType.class))
                    .productId("product_" + SECURE_RANDOM.nextInt(20))
                    .productPrice(Double.valueOf(String.format("%.2f", Math.random() * 100)))
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
