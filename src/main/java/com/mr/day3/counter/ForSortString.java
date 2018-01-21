package com.mr.day3.counter;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/17.
 */
public class ForSortString {
    public static class  ForMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }
    public static class ForReducer extends Reducer<Text,NullWritable,IntWritable,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
             String str=key.toString().trim();
            super.reduce(new Text(str), values, context);
        }
    }

    public static class MyIntComparator extends  IntWritable.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static class MyTextComparator extends  Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            System.out.println(s1);
            String str1=new String (b1,s1+1,l1-1);//b1 的大小是100M太大了
            String str2=new String(b2,s2+1,l2-1);
            System.out.println("=="+str1+"==");
            if(str1.length()==str2.length()){
                return Integer.parseInt(str1)-Integer.parseInt(str2);
            }else {
                return str2.length() - str1.length();
            }
        }
    }

    public static class  MyStringCom extends  Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            String s=new String (b1);
            System.out.println(s);
            return super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(ForSortString.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\phoneFlow\\forTestSort","",new MyTextComparator());
    }
}
