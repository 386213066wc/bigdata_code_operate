package cn.extend;

public class MainEntry {
    public static void main(String[] args) {

/*        WebEnginer webEnginer = new WebEnginer();
        webEnginer.work();*/

        Developer developer = new Developer() {
            @Override
            public void work() {
                System.out.println("自己开始工作");
            }

            @Override
            public void sleep() {

            }
        };

        /**
         * 子类的对象，指向父类的引用地址，叫做多态
         * 主要用于多个子类之间同一个父类的引用
         */
        Developer imAbstract = new ImAbstract();

    }

}
