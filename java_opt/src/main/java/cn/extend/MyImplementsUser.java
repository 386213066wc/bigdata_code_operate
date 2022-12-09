package cn.extend;

public class MyImplementsUser  implements UserInterface{
    @Override
    public void sayHello() {
        System.out.println("hello user");
        //找实现
        UserInterface userInterface =new MyImplementsUser();

        //自己实现
        UserInterface userInterface1 = new UserInterface() {
            @Override
            public void sayHello() {
                System.out.println("hellow orld");
            }
        };
        userInterface1.sayHello();


    }
}
