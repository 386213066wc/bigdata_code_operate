package cn.arrays;


import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;

/*
 * public static String toString(int[] a):把数组转成字符串
 * public static void sort(int[] a):对数组进行升序排序
 */
public class ArraysDemo {
    public static void main(String[] args) {
        //定义一个数组
        int[] arr = {24,69,80,57,13};

        Date date = new Date();//yyyy-MM-dd HH:mm:ss

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String format1 = simpleDateFormat.format(date);
        System.out.println(format1);


        System.out.println(date.toString());
        LocalDateTime now = LocalDateTime.now();
        String format = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println(format);


        System.out.println(now.toString());


        String[] strArr = {"hello","10","world"};

        //public static String toString(int[] a):把数组转成字符串
        System.out.println("排序前："+ Arrays.toString(arr));

        //public static void sort(int[] a):对数组进行升序排序
        Arrays.sort(arr);

        System.out.println("排序后："+Arrays.toString(arr));
    }
}