package cn.javaopt;

import java.util.Scanner;

public class Condition {

    public static void main(String[] args) {
        System.out.println("开始");
        System.out.println("语句A");
        System.out.println("语句B");
        System.out.println("语句C");
        System.out.println("结束");

        int a = 10;
        int b = 20;

        if (a > b) {
            System.out.println("a > b");
        } else if (a == b) {
            System.out.println("a ==b ");
        } else {
            System.out.println("a <= b ");
        }


        // 创建键盘录入数据
        Scanner sc = new Scanner(System.in);

        // 给出提示
        System.out.println("请输入一个整数(1-7)：");
        int weekDay = sc.nextInt();

        // 用switch语句实现判断
        switch (weekDay) {
            case 1:
                System.out.println("星期一");
                break;
            case 2:
                System.out.println("星期二");
                break;
            case 3:
                System.out.println("星期三");
                break;
            case 4:
                System.out.println("星期四");
                break;
            case 5:
                System.out.println("星期五");
                break;
            case 6:
                System.out.println("星期六");
                break;
            case 7:
                System.out.println("星期日");
                break;
            default:
                System.out.println("你输入的数据有误");
                break;




        }
    }

}
