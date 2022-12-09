package cn.collection;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class CollectionOpt {
    public static void main(String[] args) {
        //定义一个set集合
        //尖括号包起来的，表示泛型，泛型就是指定里面装的数据的类型
        Set<String> set = new HashSet<String>();  //多态

        set.add("hello");
       //  set.add(123);
        set.add("hello");  //去重
        Iterator iterator = set.iterator();
        while(iterator.hasNext()){
            Object next = iterator.next();
            System.out.println(next);
        }

    }



}
