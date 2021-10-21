package com.github.flink.study.other.jackson;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JacksonDemo
{
    public static void main(String[] args)
            throws JsonProcessingException
    {
        ObjectMapper mapper = new ObjectMapper();
        //使用@JsonIgnoreProperties(ignoreUnknown = true) 此项配置就没用了，一般不配置
        //mapper.configure(DeserializationFeature.FAIL_ON_IGNORED_PROPERTIES, false);

        String jsonAsString = "{\"a\":1,\"b\":2,\"c\":3}";

        MyDTO myDTO = mapper.readValue(jsonAsString, MyDTO.class);
    }
}
