package com.mr.day2.phone;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/16.
 */
public class MacFlow  implements Writable {
     private String mac;
     private int upFlow;
     private int downFlow;
     private int allFlow;
    public String getMac() {
        return mac;
    }
    public void setMac(String mac) {
        this.mac = mac;
    }
    public int getUpFlow() {
        return upFlow;
    }
    public void setUpFlow(int upFlow) {
        this.upFlow = upFlow;
    }
    public int getDownFlow() {
        return downFlow;
    }
    public void setDownFlow(int downFlow) {
        this.downFlow = downFlow;
    }
    public int getAllFlow() {
        return upFlow+downFlow;
    }
    public void setAllFlow(int allFlow) {
        this.allFlow = allFlow;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(mac);
        out.writeInt(allFlow);
        out.writeInt(upFlow);
        out.writeInt(downFlow);

    }
    @Override
    public void readFields(DataInput in) throws IOException {
        this.mac=in.readUTF();
        this.allFlow=in.readInt();
        this.upFlow=in.readInt();
        this.downFlow=in.readInt();

    }
}
