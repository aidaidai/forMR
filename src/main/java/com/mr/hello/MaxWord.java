package com.mr.hello;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class MaxWord {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text  lastWord=new Text();
        private int  max;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str [] =value.toString().split("\t");
            String word=str[0];
            int times=Integer.parseInt(str[1]);
            if(times>max){ //如果当前次数比历史次数大
                lastWord.set(word);
                max=times;
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.err.println("==========================================================================================");
            context.write(lastWord,new IntWritable(max));
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Path path=new Path("E://out");
        Job job= Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("E:\\OracleXLH\\大数据教学内容\\hadoop\\forTestData\\maxWord"));
        FileSystem fs=FileSystem.get(new URI("file://E://out"),new Configuration());
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        FileOutputFormat.setOutputPath(job,path);
        job.waitForCompletion(true);
    }

}
