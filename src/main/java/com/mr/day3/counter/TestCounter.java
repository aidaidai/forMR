package com.mr.day3.counter;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/17.
 */
public class TestCounter {



    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           Counter counter= context.getCounter(com.mr.day3.counter.Counter.ERROR);
           counter.increment(1L);
           context.getCounter("xhl","word").increment(1L);
        }
    }

    public static class ForReducer extends Reducer<Text,Text,Text,Text>{}

    public static void main(String[] args) {
        JobUtil.commitJob(TestCounter.class,"E:\\OracleXLH\\大数据教学内容\\hadoop\\按天计算的教学内容\\05day","");
    }
}
