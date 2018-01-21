package com.mr.day3.join;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 代俊朴 on 2017/12/14.
 */
public class Info implements Writable{
    private String orderId=""; //订单编号
    private String dateTime=""; //日期
    private int amount; //数量
    private String pId="";//商品ID
    private String pName="";
    private int  price;
    private boolean  flag; // 决定改实体当前决定的值是 商品还是订单 false 订单 true 商品

    @Override
    public void write(DataOutput out) throws IOException {
          out.writeUTF(orderId);
          out.writeUTF(dateTime);
          out.writeInt(amount);
          out.writeUTF(pId);
          out.writeUTF(pName);
          out.writeInt(price);
          out.writeBoolean(flag);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.orderId=in.readUTF();
        this.dateTime=in.readUTF();
        this.amount=in.readInt();
        this.pId=in.readUTF();
        this.pName=in.readUTF();
        this.price=in.readInt();
        this.flag=in.readBoolean();

    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getDateTime() {
        return dateTime;
    }

    public void setDateTime(String dateTime) {
        this.dateTime = dateTime;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getpId() {
        return pId;
    }

    public void setpId(String pId) {
        this.pId = pId;
    }

    public String getpName() {
        return pName;
    }

    public void setpName(String pName) {
        this.pName = pName;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    /**
     * false 是订单
     * @return
     */
    public boolean isFlag() {
        return flag;
    }
    /**
     * false 是订单
     * @return
     */
    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return  "   pId='" + pId + '\'' +
                "   orderId='" + orderId + '\'' +
                "   dateTime='" + dateTime + '\'' +
                "   amount=" + amount +
                "   pName='" + pName + '\'' +
                "   price=" + price
                ;
    }
}
