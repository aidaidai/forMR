package com.mr.hello;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by 代俊朴 on 2018/1/15.
 */
public class WordCount {
     public static  class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
         private Text okey=new Text();
         private IntWritable intWritable=new IntWritable(1);
         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                 String str []=value.toString().split(" ");
                 for(String s: str){
                     okey.set(s);
                     context.write(okey,intWritable);
                 }
         }
     }
    /**
     * 这里接收到的应该是 字母和 数字1
     */
    public static class ForReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
         private IntWritable ovalue=new IntWritable();
         @Override
         protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
             int  sum=0;
             for(IntWritable i: values){
                  sum++;
              }
              ovalue.set(sum);
              context.write(key,ovalue);
         }
     }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Path path=new Path("E://out");
        Job job= Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\OracleXLH\\大数据教学内容\\hadoop\\forTestData\\forWordCount"));
        FileSystem  fs=FileSystem.get(new URI("file://E://out"),new Configuration());
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);
        job.waitForCompletion(true);
    }


}
