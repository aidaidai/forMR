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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by 代俊朴 on 2018/1/15.
 */
public class WordCountTopN {
    public static  class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        private List<WordTimes> list=new ArrayList<>(4);
        {  for(int i=0;i<3;i++)
            list.add(new WordTimes("",0));
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              String str [] =value.toString().split("\t");
              WordTimes wt=new WordTimes(str[0],Integer.parseInt(str[1]));
              list.add(wt);
              Collections.sort(list);
             list.remove(3);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i=0;i<list.size();i++){
                WordTimes wordTimes=list.get(i);
                System.err.println(wordTimes);
                context.write(new Text(wordTimes.getWord()),new IntWritable(wordTimes.getNum()));
            }
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
