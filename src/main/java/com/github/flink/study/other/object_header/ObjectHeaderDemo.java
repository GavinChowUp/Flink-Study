package com.github.flink.study.other.object_header;

import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.NavigableSet;
import java.util.TreeSet;

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
        ArrayList<String> strings = new ArrayList<>();
        strings.add("1");
        strings.add("R1");
        strings.add("1");

        TreeSet<Integer> set1 = new TreeSet<>();
        TreeSet<Integer> set2 = new TreeSet<>();

        set1.add(1);
        set1.add(3);

        set2.add(0);
        set2.add(4);

        set1.addAll(set2);

        LinkedList<Integer> integers = Lists.newLinkedList(set1);

        NavigableSet<Integer> integers1 = set1.descendingSet();

        System.out.println(integers1);
        System.out.println(integers);

        System.out.println(set1);
    }
}
