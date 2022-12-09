package cn.extend;

//通过extend关键字，继承了一个父类，就继承了父类的属性以及方法
public class Student extends Person{

    public static void main(String[] args) {
        Student student = new Student();
        student.setName("hello");



    }

    public void sayHello(){
        int age = super.getAge();
    }


    /**
     * 表示重写
     */
    @Override
    public void sayABC() {
        System.out.println("hello world");
    }
}
