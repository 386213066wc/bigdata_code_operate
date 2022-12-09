package cn.api;

public class StringBuilderDemo {

    public static void main(String[] args) {
        //线程不安全的
        StringBuilder stringBuilder = new StringBuilder();
        System.out.println(stringBuilder);
        StringBuilder helloworld = stringBuilder.append("helloworld");

        StringBuilder append = helloworld.append(9).append(6.5d);
        append.delete(0,3);
        append.insert(5,"helloworld");
        append.indexOf("wo");


    }
}
