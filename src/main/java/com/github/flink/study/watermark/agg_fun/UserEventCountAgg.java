package com.github.flink.study.watermark.agg_fun;

import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class UserEventCountAgg
        implements AggregateFunction<UserEvent, UserEventCount, UserEventCount>
{
    @Override
    public UserEventCount createAccumulator()
    {
        return UserEventCount.builder().count(0).build();
    }

    @Override
    public UserEventCount add(UserEvent value, UserEventCount accumulator)
    {
        return UserEventCount.builder().userId(value.getUserId()).count(accumulator.getCount() + 1).build();
    }

    @Override
    public UserEventCount getResult(UserEventCount accumulator)
    {
        return accumulator;
    }

    @Override
    public UserEventCount merge(UserEventCount a, UserEventCount b)
    {
        return UserEventCount.builder().userId(a.getUserId()).count(a.getCount() + b.getCount()).build();
    }
}
