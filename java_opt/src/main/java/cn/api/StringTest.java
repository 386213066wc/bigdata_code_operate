package cn.api;

import java.util.Scanner;

public class StringTest {

    public static void main(String[] args) {
        //定义两个字符串对象，用于存储已经存在的用户名和密码
        String username = "admin";
        String password = "admin";

        for(int x=0; x<3; x++) {
            //键盘录入用户名和密码
            Scanner sc = new Scanner(System.in);
            System.out.println("请输入用户名：");
            String name = sc.nextLine();
            System.out.println("请输入密码：");
            String pwd = sc.nextLine();

            //拿键盘录入的用户名和密码去跟已经存在的用户名和密码进行比较
            if(username.equals(name) && password.equals(pwd)) {
                System.out.println("登录成功");
                break;
            }else {
                if((2-x) == 0){
                    System.out.println("你的帐号被锁定，请与管理员联系");
                }else {
                    System.out.println("登录失败，你还有"+(2-x)+"次机会");
                }
            }
        }
    }
}
