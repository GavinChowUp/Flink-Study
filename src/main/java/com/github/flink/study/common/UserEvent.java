package com.github.flink.study.common;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder()
public class UserEvent
{
    private Integer userID;
    private LocalDateTime eventTime;
    private String eventType;
    private Integer productID;
    private Double productPrice;
}
