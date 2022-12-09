package cn.extend;

/*I*
Cat 继承自Animal
 */
public class Cat extends Animal {
    public int age = 20;
    public int weight = 10;

    public void eat() {
        System.out.println("猫吃鱼");
    }

    public void playGame() {
        System.out.println("猫捉迷藏");
    }
}
