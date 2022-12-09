package cn.javaopt;

public class MethodOpt {

    /**
     * 特殊的方法
     * @param args
     */
    public static void main(String[] args) {

    }





    //  [访问权限修饰符]  [其他修饰符]  返回值类型    方法名称 （方法的参数） {
    //    方法的方法体
    //  }

    // 定义一个方法，使用public，没有其他修饰符，返回int类型，方法名称叫做hello  方法的参数带一个int a

    public int hello(int a ){
        return a;
    }


    /**
     * 方法的方法名称不能重复，除非参数列表不一样，叫做方法的重载
     * @param a
     * @return
     */
    public int hello(int a,int b  ){
        if (a ==b){
        hello(a,b );  //方法内存，自己调用自己，叫做方法的递归，递归一定要小心，一定要有终止值
        }else{

        }
        return a;
    }





}
