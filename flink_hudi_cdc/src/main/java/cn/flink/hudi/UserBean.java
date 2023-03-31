package cn.flink.hudi;


public class UserBean {
    //id INT ,   NAME STRING,  age INT
    private int id;
    private String NAME;
    private int age;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getNAME() {
        return NAME;
    }

    public void setNAME(String NAME) {
        this.NAME = NAME;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "UserBean{" +
                "id=" + id +
                ", NAME='" + NAME + '\'' +
                ", age=" + age +
                '}';
    }
}
