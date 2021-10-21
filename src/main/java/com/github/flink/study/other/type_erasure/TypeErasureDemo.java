package com.github.flink.study.other.type_erasure;

import java.util.ArrayList;

public class TypeErasureDemo
{
    public static void main(String[] args)
    {
        ArrayList<String> stringsErased = new ArrayList<>();
        ArrayList<Integer> integersErased = new ArrayList<>();

        //true: 说明string 和integer的类型被擦除了
        //泛型帮助判断编译期类型错误
        System.out.println(stringsErased.getClass().equals(integersErased.getClass()));

        ArrayList<String> strings = new ArrayList<String>() {};
        ArrayList<Integer> integers = new ArrayList<Integer>() {};

        System.out.println(strings.getClass().equals(integers.getClass()));
    }
}
