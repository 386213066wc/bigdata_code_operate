package cn.collection;

import java.util.*;

public class ListOpt {
    public static void main(String[] args) {

        //底层使用的是一个变长的数组，线程不安全的，数据的增删慢，查找快
        List list = new ArrayList();
        list.add("world");
        list.add("hello");
        list.add("hello");


        for(int i = 0;i<list.size();i++){
            System.out.println(list.get(i));
        }

      /*  Iterator iterator = list.iterator();
        while(iterator.hasNext()){
            iterator.next();
        }*/
        // 擅长做删除或者频繁的插入 链表来实现的
        LinkedList<String> linkList = new LinkedList<String>();
        linkList.add("a");
        linkList.add("b");
        linkList.add("c");
        linkList.add("d");

        System.out.println(linkList.getFirst());
        System.out.println(linkList.getLast());
        System.out.println(linkList.removeFirst());
        System.out.println(linkList.removeLast());
        boolean empty = linkList.isEmpty();
        Collections.sort(linkList);


    }
}
