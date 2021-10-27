package com.github.flink.study.topn;

import lombok.Data;

import java.util.Comparator;
import java.util.TreeSet;

public class TreeSetDemo
{
    public static void main(String[] args)
    {
        TreeSet<SetPoJo> set = new TreeSet<>(Comparator.comparing(SetPoJo::getNum));

        set.add(new SetPoJo(1, 1));
        set.add(new SetPoJo(1, 2));
        set.add(new SetPoJo(1, 3));

        System.out.println(set);
    }

    @Data
    private static class SetPoJo
    {
        private int num;
        private int time;

        public SetPoJo(int num, int time)
        {
            this.num = num;
            this.time = time;
        }
    }
}
