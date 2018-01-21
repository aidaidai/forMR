package com.mr.day2.phone;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/16.
 */
public class Flow  implements WritableComparable<Flow>{
    private String phoneNumber;
    private int allFlow;
    private int downFlow;
    private int upFlow;

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public int getAllFlow() {
        return allFlow;
    }

    public void setAllFlow(int allFlow) {
        this.allFlow = allFlow;
    }

    public int getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }

    public int getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(allFlow);
        dataOutput.writeInt(downFlow);
        dataOutput.writeInt(upFlow);
        dataOutput.writeUTF(phoneNumber);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.allFlow=dataInput.readInt();
        this.downFlow=dataInput.readInt();
        this.upFlow=dataInput.readInt();
        this.phoneNumber=dataInput.readUTF();
    }

    @Override
    public String toString() {
        return "Flow{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", allFlow=" + allFlow +
                ", downFlow=" + downFlow +
                ", upFlow=" + upFlow +
                '}';
    }

    @Override
    public int compareTo(Flow o) {
        if(o.allFlow==this.allFlow){
            return o.downFlow-this.downFlow;
        }
        return o.allFlow-this.allFlow;
    }
}
