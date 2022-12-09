package cn.javaopt;

public class LoopOpt {

    public static void main(String[] args) {
        int a = 100;

        for(int i = 0;i <= a  ;i ++ ){
            if(i == 50){
                break;
            }
            System.out.println(i );

        }

        while(a <= 200){
            System.out.println(a);
            a +=1 ;
        }


    }

}
