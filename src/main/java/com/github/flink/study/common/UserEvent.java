package com.github.flink.study.common;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Builder()
public class UserEvent
{
    private String userID;
    private LocalDateTime eventTime;
    private UserEventType userEventType;
    private String productID;
    private Double productPrice;
}
