package com.github.flink.study.util;

import com.github.flink.study.common.UserEvent;
import com.github.flink.study.common.UserEventType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.security.SecureRandom;
import java.util.Random;

public class EventBuilder
{
    private static final Random SECURE_RANDOM = new SecureRandom();

    public static <T extends Enum<T>> T randomEnum(Class<T> clazz)
    {
        int x = SECURE_RANDOM.nextInt(clazz.getEnumConstants().length);
        return clazz.getEnumConstants()[x];
    }

    public static UserEvent BuildUserEvent()
    {
        long l = System.currentTimeMillis();
        return UserEvent.builder()
                .userId("user_" + 1)
                .stringEventTime(new DateTime(l, DateTimeZone.forID("+08:00")).toString("yyyy-MM-dd HH:mm:ss"))
                .eventTime(l)
                .userEventType(randomEnum(UserEventType.class))
                .productId("product_" + SECURE_RANDOM.nextInt(20))
                .productPrice(Double.valueOf(String.format("%.2f", Math.random() * 100)))
                .build();
    }
}
