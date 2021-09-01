package com.github.flink.study.watermark.agg_fun;

import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class UserEventCountWithTimeFieldAgg
        implements AggregateFunction<UserEvent, UserEventCountWithTimeField, UserEventCountWithTimeField>
{
    @Override
    public UserEventCountWithTimeField createAccumulator()
    {
        return UserEventCountWithTimeField.builder().count(0).build();
    }

    @Override
    public UserEventCountWithTimeField add(UserEvent value, UserEventCountWithTimeField accumulator)
    {
        return UserEventCountWithTimeField.builder().userId(value.getUserId()).count(accumulator.getCount() + 1).build();
    }

    @Override
    public UserEventCountWithTimeField getResult(UserEventCountWithTimeField accumulator)
    {
        return accumulator;
    }

    @Override
    public UserEventCountWithTimeField merge(UserEventCountWithTimeField a, UserEventCountWithTimeField b)
    {
        return UserEventCountWithTimeField.builder().userId(a.getUserId()).count(a.getCount() + b.getCount()).build();
    }
}
