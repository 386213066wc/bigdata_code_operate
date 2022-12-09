package cn.collection;

import java.util.HashMap;
import java.util.Map;

public class MapCollection {
    public static void main(String[] args) {
        //定义map集合，map集合是key，value对的
        Map<Object,Object> map = new HashMap<Object,Object>();

        //map集合的key，是唯一的不能重复，且不能为null值
        map.put("hello","world");
        map.put("hello","abc");
        for (Object s : map.keySet()) {
            System.out.println("key为" + s);
            System.out.println("value为"+map.get(s));
        }
    }

}
