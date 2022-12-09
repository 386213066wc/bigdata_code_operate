package cn.javaopt;

public class Compute {
    public static void main(String[] args) {


       /* //定义两个int类型的变量
        int a = 5;
        int b = 3;
        int c = b++; //C的值依然是3
        int d = ++b; //d的值5

        System.out.println(a+b);
        System.out.println(a-b);
        System.out.println(a*b);
        System.out.println(a/b);
        System.out.println(a%b);
        System.out.println(c);
        System.out.println(d);
        System.out.println("------------");

        System.out.println(5/4);
        System.out.println(5.0/4);
        System.out.println(5/4.0);*/


      /*  //定义变量
        int a = 10; //把10赋值给int类型的变量a
        System.out.println("a:"+a);

        //扩展的赋值运算符：+=
        //把运算符左边的数据和右边的数据进行运算，然后把结果赋值给左边
        //a = a + 210;
        a += 10;  // a= a + 10   a += 10
        System.out.println("a:"+a);

        //short s = 1;
        //s = s + 1;

        //扩展的赋值运算符隐含了强制类型转换。
        //a+=20
        //等价于
        //a =(a的数据类型)(a+20);
        short s = 1;
        s += 1;
        System.out.println("s:"+s);
*/

        boolean a = true;
        boolean b = true;
        System.out.println(a && b );

        boolean c = false;
        boolean d = true;
        System.out.println(c || d );


        int e = 10;
        int f = 20;
        int g = (e > f) ? e : f;
        System.out.println(g);





    }

}
