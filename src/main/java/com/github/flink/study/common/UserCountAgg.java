package com.github.flink.study.common;

import org.apache.flink.api.common.functions.AggregateFunction;

public class UserCountAgg
        implements AggregateFunction<UserEvent, Double, String>
{
    @Override
    public Double createAccumulator()
    {
        return 0d;
    }

    @Override
    public Double add(UserEvent value, Double accumulator)
    {
        return accumulator + 1;
    }

    @Override
    public String getResult(Double accumulator)
    {
        return "User count:" + accumulator;
    }

    @Override
    public Double merge(Double a, Double b)
    {
        return a + b;
    }
}
