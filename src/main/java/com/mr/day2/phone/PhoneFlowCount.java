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
public class PhoneFlowCount {
     public static class ForMapper extends Mapper<LongWritable,Text,Text,LongWritable>{
         private Text okey=new Text();
         private LongWritable ovalue=new LongWritable();
         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String str [] =value.toString().split("\t");
                int upFlow=Integer.parseInt(str[8]);
                int downFlow=Integer.parseInt(str[9]);
                long allFlow=upFlow+downFlow;
                String phoneNumber=str[1];
                okey.set(phoneNumber);
                ovalue.set(allFlow);
                context.write(okey,ovalue);
         }
     }
     public static class ForReducer extends Reducer<Text,LongWritable,Text,LongWritable>{
         private LongWritable ovalue=new LongWritable();
         @Override
         protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
               long  sumFlow=0;
               for(LongWritable l:values){
                   sumFlow+=l.get();
               }
               ovalue.set(sumFlow);
               context.write(key,ovalue);
         }
     }
    public static void main(String[] args) {
        JobUtil.commitJob(PhoneFlowCount.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\phoneFlow\\data","");
    }
}
