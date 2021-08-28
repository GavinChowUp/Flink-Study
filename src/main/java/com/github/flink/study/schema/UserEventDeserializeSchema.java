package com.github.flink.study.schema;

import com.github.flink.study.common.UserEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class UserEventDeserializeSchema
        implements DeserializationSchema<UserEvent>
{
    private final ObjectMapper objectMap = new ObjectMapper();

    @Override
    public void open(InitializationContext context)
            throws Exception
    {
        DeserializationSchema.super.open(context);
    }

    @Override
    public UserEvent deserialize(byte[] message)
            throws IOException
    {
        return objectMap.readValue(message, UserEvent.class);
    }

    @Override
    public boolean isEndOfStream(UserEvent nextElement)
    {
        return false;
    }

    @Override
    public TypeInformation<UserEvent> getProducedType()
    {
        return TypeInformation.of(UserEvent.class);
    }
}
