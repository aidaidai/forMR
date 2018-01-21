package com.mr.day2.phone;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/16.
 */
public class PhoneFlowAllCount {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Flow>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs[]=value.toString().split("\t");
            int upFlow=Integer.parseInt(strs[8]);
            int downFlow= Integer.parseInt(strs[9]);
            int allFlow=upFlow+downFlow;// 总流量
            String phoneNumber=strs[1];
            Flow flow=new Flow();
            flow.setAllFlow(allFlow);
            flow.setDownFlow(downFlow);
            flow.setUpFlow(upFlow);
            flow.setPhoneNumber(phoneNumber);
            context.write(new Text(phoneNumber),flow);
        }
    }
    public static class ForReducer extends Reducer<Text,Flow,Text,Flow>{
        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
            int upFlow=0;
            int downFlow= 0;
            for(Flow f:values){
                upFlow+=f.getUpFlow();
                downFlow+=f.getDownFlow();
            }
            System.out.println("");
            int allFlow=upFlow+downFlow;// 总流量
            Flow flow=new Flow();
            flow.setAllFlow(allFlow);
            flow.setDownFlow(downFlow);
            flow.setUpFlow(upFlow);
            context.write(key,flow);
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(PhoneFlowAllCount.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\phoneFlow\\data","");
    }
}
