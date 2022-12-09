package cn.javaopt;

import java.util.Collections;

/**
 * 可以使用数组来装很多值
 */
public class ArrayOpt {

    public static void main(String[] args) {
        //定义数组有好几种方式
        //第一种方式，初始化一个数组，指定数组的长度
        int [] number = new int[3];
        number[0] = 10;
        number[1] =20;
        number[2] =30;
      //  number[3]=40;
        System.out.println(number.toString());

        for (int i : number) {
            System.out.println(i);
        }


        //第二种定义方式，定义数组，直接指定数组内的元素
        int [] number2 = new int[]{1,2,3,4,5,6};
        for (int i : number2) {
            System.out.println(i);
        }
        int length = number2.length;



        for(int i = 0;i< number2.length;i ++){
            System.out.println(number2[i]);
        }


        int[] array = {1, 3, 5, 7, 9, 0, 8, 6, 4, 2};


        for (int index = 0; index < array.length - 1; index++) {
            for (int compare = index + 1; compare < array.length; compare++) {
                if (array[index] < array[compare]) {
                    int temp = array[index];
                    array[index] = array[compare];
                    array[compare] = temp;
                }
            }
        }



    }

}
