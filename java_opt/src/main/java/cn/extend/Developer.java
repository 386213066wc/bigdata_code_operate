package cn.extend;

/**
 * 使用abstract修饰的就是一个抽象类
 */
public abstract class Developer   {
    int a = 10;

    public void sayHello(){
        System.out.println("Abctest");
    }

    //使用 abstract来修饰的就是抽象方法，抽象方法不能有方法体
    public abstract void work();


    public  abstract  void sleep();



}
