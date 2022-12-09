package cn.api;

import java.io.PrintStream;
import java.util.Random;

public class JavaOpt {

    public static void main(String[] args) {
        Random random = new Random();
        for(int i =0;i<=100;i++){
            int j = random.nextInt();
            System.out.println(j);
        }


        //基本数据类型，有默认值，不可能为null值
        int p = 0;
        System.out.println(p);
        //包装类型，没有默认值，可能为null值
        Integer b = null;

        //将基础数据类型转换成为包装类型，叫做装箱
        int a = 10;
        // 通过构造方法进行装箱
        Integer i1 = new Integer(a);
        // 通过valueOf方法进行装箱
        Integer i2 = Integer.valueOf(a);


        //将包装类型转换成为基础类型，叫做拆箱
        Integer i = new Integer(10);
        int bb = i.intValue();


        Student student = new Student();
        SmallStudent smallStudent = new SmallStudent();
        if (smallStudent.equals(student)){

        }


    }

}
