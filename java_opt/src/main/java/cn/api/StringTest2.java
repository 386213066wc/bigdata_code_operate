package cn.api;

import java.io.UnsupportedEncodingException;

public class StringTest2 {

    public static void main(String[] args) throws UnsupportedEncodingException {

        String s1 = "hello";
        String s2 = "world   ";
        System.out.println(s1 + s2);
        s2 += s1 + s2;//String定义的字符串是不可变的，就算你给它改变了值，在底层也是进行了重新创建了一个String对象
        System.out.println(s2);

        //字符串反转
        System.out.println(reverse(s2));


        String trim = s2.trim();
        byte[] bytes = s2.getBytes("UTF-8");

        String s = s2.toLowerCase();
        String s3 = s2.toUpperCase();
        String[] hs = s2.split("h", 5);
        s2.contains("wo");
        s2.startsWith("o");


    }

    //将反转之后的字符串返回回去
    public static String reverse(String s){
        String ss = "";
        for(int x = s.length()-1;x>=0;x--){
            ss += s.charAt(x);

        }
        return ss;

    }



}
