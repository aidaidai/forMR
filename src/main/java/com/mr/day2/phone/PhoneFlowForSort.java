package com.mr.day2.phone;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/16.
 */
public class PhoneFlowForSort {
    public static  class ForMapper  extends Mapper<LongWritable,Text, Flow,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String strs[] =line.split("\t");
            Flow  flow=new Flow();
            flow.setUpFlow(Integer.parseInt(strs[3]));
            flow.setDownFlow(Integer.parseInt(strs[2]));
            flow.setAllFlow(Integer.parseInt(strs[1]));
            flow.setPhoneNumber(strs[0]);
            context.write(flow,NullWritable.get());
        }
    }
    public static class  ForReducer extends Reducer<Flow,NullWritable,Flow,NullWritable> {
        @Override
        protected void reduce(Flow key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException {
        Job  job= Job.getInstance();


        JobUtil.commitJob(PhoneFlowForSort.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\phoneFlow\\forSort","");
    }
}
