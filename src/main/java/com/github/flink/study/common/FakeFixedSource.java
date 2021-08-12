package com.github.flink.study.common;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

public class FakeFixedSource
        implements SourceFunction<UserEvent>
{
    private static final Random SECURE_RANDOM = new SecureRandom();
    private volatile boolean isRunning = true;

    private long sleepMillis;

    public FakeFixedSource()
    {
        this(3000L);
    }

    private FakeFixedSource(long sleepMills)
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
                    .userId("user_" + SECURE_RANDOM.nextInt(10))
                    .eventTime(LocalDateTime.now().minusSeconds(SECURE_RANDOM.nextInt(3)).toEpochSecond(ZoneOffset.of("+08:00")))
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
