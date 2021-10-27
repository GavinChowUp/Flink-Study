package com.github.flink.study.topn;

import java.util.Comparator;
import java.util.PriorityQueue;

public class PriorityQueueDemo
{
    public static void main(String[] args)
    {
        PriorityQueue<Integer> integers = new PriorityQueue<>(5, Comparator.comparingInt(a -> a));

        integers.add(1);
        integers.add(2);
        integers.add(3);
        integers.add(1);
        integers.add(5);
        integers.add(6);
        integers.add(9);
        System.out.println(integers);
    }
}
