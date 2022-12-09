package cn.javaopt;

/**
 * 定义用户类
 */
public  class User {

    /**
     * 人的一些基本属性
     * 访问权限修饰符
     * private  表示只能在本类当中使用
     * protected   表示只能在本包当中使用
     * 默认 一般不会用
     * public 表示任意其他包当中都可以访问
     *
     */
    private   static int  arm = 2;
    int leg = 2;
    int head = 1;

    /**
     * 行为
     */
    public    void eat(){
        //this= new User();
        int head = this.head;
        this.sleep();
        System.out.println("吃饭吃饭吃饭");
    }

    public void sleep(){
        System.out.println("shuijiao");
    }



    //使用static修饰的方法或者属性，可以直接调用
    public static void walk(){
        System.out.println("开始行走");
    }


    //通过类来创建对象
    public static void main(String[] args) {
       // this.eat();

        //如果使用了static修饰的属性或者方法，可以直接调用，不用创建对象
        //直接使用类来访问 方法或者属性，不需要对象来访问
        User.walk();
        int arm1 = User.arm;


        User user = new User();  //通过类 使用new关键字来创建对象
        //以后写的所有的java代码都是通过创建对象，使用new，如果不能new，想其他办法，一定要得到对象
        user.eat();

        int arm = user.arm;

    }

}
