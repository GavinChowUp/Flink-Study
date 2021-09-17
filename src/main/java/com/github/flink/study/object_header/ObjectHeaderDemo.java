package com.github.flink.study.object_header;

import org.openjdk.jol.info.ClassLayout;

public class ObjectHeaderDemo
{
    public static void main(String[] args)
    {
        BooleanFlag booleanFlag = new BooleanFlag();
        System.out.println(ClassLayout.parseInstance(booleanFlag).toPrintable());

        System.out.println(1 % 10);
        System.out.println(2 % 10);
        System.out.println(3 % 10);
        System.out.println(4 % 10);
        System.out.println(5 % 10);
        System.out.println(6 % 10);
    }
}
