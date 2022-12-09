package cn.javaopt;

/**
 * 定义了一个java文件
 * 我是注释，这个注释不会被执行
 *
 */
public class HelloWorld {
    //定义一个常量值，不让改变的值
    public  static final String abc = "abc";

    public  static  String abcd = "abc";

    //程序的入口类
    public static void main(String[] args) {

        abcd = "200";


        System.out.println("hello world");

        int userId = 10;

        int c2=20;

        int abc_test  =30;

        byte b = 100;
        System.out.println(b);

        //short 类型的变量
        short s = 10000;

        System.out.println(s);

        int i = 1000000000;

        boolean b1 = true;
        b1 = false ;
        char ch = 's';


        int j = 10000;
        //进行强制类型转换，这个时候会出现精度丢失的问题

        //大范围向小范围转换，可能存在精度丢失的问题，小范围向大范围转换，会不会有精度丢失问题？？？
        byte by = (byte)j;

        byte by2 = 100;
        int byi = by2;






    }




}
