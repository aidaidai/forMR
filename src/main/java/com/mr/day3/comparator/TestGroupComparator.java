package com.mr.day3.comparator;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/19.
 */
public class TestGroupComparator {

   public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
       private Text okey=new Text();
       @Override
       protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String strs [] = value.toString().split(" ");
           okey.set(strs[0]);
           context.write(okey,new IntWritable(Integer.parseInt(strs[1])));
       }
   }
   public static class ForReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
       @Override
       protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
              int sum=0;
              for(IntWritable i:values){
                  sum+=i.get();
              }
           System.out.println(key.toString());
              key.set(key.toString().substring(0,1));
              context.write(key,new IntWritable(sum));
       }
   }
   public static class MyGroupComparator extends WritableComparator{
       public MyGroupComparator() {
           super(Text.class,true);
       }
       @Override
       public int compare(WritableComparable a, WritableComparable b) {
           System.out.println("---------------------------------");
           System.out.println(a.toString().substring(0,1));
           System.out.println(b.toString().substring(0,1));
           System.out.println("---------------------------------");
           return  a.toString().substring(0,1).compareTo(b.toString().substring(0,1));
   }
   }
    public static void main(String[] args) {
        JobUtil.commitJob(TestGroupComparator.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\gpinput",
                "",new MyGroupComparator());
    }
}


















