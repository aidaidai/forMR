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
public class ForSort {
    public static class  ForMapper extends Mapper<LongWritable,Text,IntWritable,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(Integer.parseInt(value.toString())), NullWritable.get());
        }
    }
    public static class ForReducer extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            super.reduce(key, values, context);
        }
    }

    public static class MyIntComparator extends  IntWritable.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
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
        JobUtil.commitJob(ForSort.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\phoneFlow\\forTestSort","",new MyIntComparator());
    }
}
