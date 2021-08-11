package com.github.flink.study.common;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

// {"userID": "user_4", "eventTime": "2019-11-09 10:41:32", "eventType": "browse", "productID": "product_1", "productPrice": 10}

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
