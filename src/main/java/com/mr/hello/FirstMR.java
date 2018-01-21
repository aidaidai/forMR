package com.mr.hello;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by 代俊朴 on 2018/1/15.
 */
public class FirstMR {
    public static class  ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key+","+value.toString());
            context.write(new Text(key+""),value);
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        String output="E://out";
        Job job= Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\OracleXLH\\大数据教学内容\\hadoop\\作业\\整理的作业\\MapReduce经典案例 WordCount 练习题及答案\\实验数据\\access.20120104.log"));
        FileOutputFormat.setOutputPath(job,new Path("E:\\out"));
        FileSystem fs=FileSystem.get(new URI("file://"+output),new Configuration());
        if(fs.exists(new Path(output))){
             fs.delete(new Path(output),true);
        }
        job.waitForCompletion(true);
    }

}
