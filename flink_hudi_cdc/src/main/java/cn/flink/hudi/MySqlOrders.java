package cn.flink.hudi;


import java.sql.Timestamp;

public class MySqlOrders {

    private int order_id;

    private Timestamp order_date;
    private String customer_name;
    private Double price;
    private int product_id;
    private boolean order_status;


    public int getOrder_id() {
        return order_id;
    }

    public void setOrder_id(int order_id) {
        this.order_id = order_id;
    }

    public Timestamp getOrder_date() {
        return order_date;
    }

    public void setOrder_date(Timestamp order_date) {
        this.order_date = order_date;
    }

    public String getCustomer_name() {
        return customer_name;
    }

    public void setCustomer_name(String customer_name) {
        this.customer_name = customer_name;
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public int getProduct_id() {
        return product_id;
    }

    public void setProduct_id(int product_id) {
        this.product_id = product_id;
    }

    public boolean isOrder_status() {
        return order_status;
    }

    public void setOrder_status(boolean order_status) {
        this.order_status = order_status;
    }

    @Override
    public String toString() {
        return "MySqlOrders{" +
                "order_date=" + order_date +
                ", customer_name='" + customer_name + '\'' +
                ", price=" + price +
                ", product_id=" + product_id +
                ", order_status=" + order_status +
                '}';
    }
}