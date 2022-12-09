package cn.extend;

public class TestDemo {
   /* public static void main(String[] args) {
        //正常情况下，创建了两个对象
        Cat cat = new Cat();
        Pig pig = new Pig("佩奇");

        //这里创建的本质还是子类的对象 子类的对象，指向了父类的引用地址，叫做多态
        Animal animal = new Cat();


        //多态
        Animal a = new Cat(); //向下转型
        Animal b = new Pig("佩奇");
        Pig d = (Pig) b;  //编译不报错，但是执行的时候会报错
        d.sayHello();





        Cat c = new Cat();
        if(a instanceof Cat){  //instance  of 主要可以用于判断两个类是否是属于同一个类型
            c = (Cat) a;
        }
        c.eat();
        c.playGame();
    }*/

    public static void main(String[] args) {
        UserInterface userInterface = new UserInterface() {
            @Override
            public void sayHello() {

            }
        };


        TestDemo testDemo = new TestDemo();
        testDemo.sayHello(new UserInterface() {
            @Override
            public void sayHello() {
                System.out.println("abc test ");
            }
        });


    }


    //这个方法传入的一个参数，是一个接口
    public void sayHello(UserInterface userInterface){

    }



}

