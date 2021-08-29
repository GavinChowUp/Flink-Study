package com.github.flink.study.common;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder()
@AllArgsConstructor
@NoArgsConstructor
public class UserEventCount
        implements Serializable
{
    private String userId;
    private Integer count;
}