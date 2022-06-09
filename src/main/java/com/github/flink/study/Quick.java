package com.github.flink.study;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.JsonNodeType;

public class Quick
{
    public static void main(String[] args)
            throws JsonProcessingException
    {
        String a = "{\"a\":null,\"b\":1}";

        ObjectMapper mapper = new ObjectMapper();

        JsonNode jsonNode = mapper.readTree(a);

        JsonNodeType nodeType = jsonNode.get("a").getNodeType();

        System.out.println(nodeType);

        System.out.println(jsonNode.get("a") != null);
    }
}