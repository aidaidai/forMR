package com.mr.day3.combiner;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 代俊朴 on 2017/9/29.
 */
public class AvgEntity implements Writable{
   private int sum;
   private int count=1;
    public AvgEntity(int sum, int count) {
        this.sum = sum;
        this.count = count;
    }
    public AvgEntity() {
    }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
         dataOutput.writeInt(sum);
        dataOutput.writeInt(count);
    }
    @Override
    public void readFields(DataInput dataInput) throws IOException {
       this.sum=dataInput.readInt();
       this.count=dataInput.readInt();
    }
    public int getSum() {
        return sum;
    }
    public void setSum(int sum) {
        this.sum = sum;
    }
    public int getCount() {
        return count;
    }
    public void setCount(int count) {
        this.count = count;
    }
}
