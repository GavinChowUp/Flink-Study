package com.github.flink.study.object_header;

import org.openjdk.jol.info.ClassLayout;

public class ObjectHeaderDemo
{
    public static void main(String[] args)
    {
        BooleanFlag booleanFlag = new BooleanFlag();
        System.out.println(ClassLayout.parseInstance(booleanFlag).toPrintable());
    }
}
