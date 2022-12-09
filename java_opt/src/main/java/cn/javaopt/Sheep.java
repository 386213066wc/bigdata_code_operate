package cn.javaopt;

public class Sheep {
    //正常情况下方法的定义  访问权限修饰符，其他修饰符，方法返回值，方法的名称  （方法的参数）{方法的方法体}

    int b= 0;
    //这是一个构造方法，很特殊的一个方法  方法的返回值与类名保持一致  主要用于创建对象，传递不同的参数用的
    public Sheep(int a){
        this.b = a ;


    }


    //也是一个构造方法
    public Sheep(int a,int c ){
        this.b = a ;


    }

    public static void main(String[] args) {
        Sheep sheep = new Sheep(10, 20);
        sheep.add_method(10,20,30);



    }

    //将一些复杂的代码给封装起来，堆外只暴露一个方法名称即可
    public void  add_method(int a,int b ,int c ){
        System.out.println(a + b + c );
    }




}
