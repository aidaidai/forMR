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
public class FirstReduce {
    public static class ForMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text okey = new Text();
        private Text ovalue = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String str[] = line.split("\t");
            if (str.length == 3) {
                System.out.println("================");
                okey.set(str[0]);
                ovalue.set(str[2]);
                context.write(okey, ovalue);
            }

        }
    }
    public static class ForReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            int sum=0;
            for (Text t : values) {
                sum+=Integer.parseInt(t.toString());
                System.out.println(t);
            }
            context.write(key,new IntWritable(sum));
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        String output="E://out";
        Job  job= Job.getInstance();
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapperClass(ForMapper.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setReducerClass(ForReducer.class);
        FileSystem  fileSystem= FileSystem.get(new URI("file://"+output),new Configuration());
        FileInputFormat.setInputPaths(job,new Path("E:\\OracleXLH\\大数据教学内容\\hadoop\\按天计算的教学内容\\04day\\classData.txt"));
        FileOutputFormat.setOutputPath(job,new Path(output));
        if(fileSystem.exists(new Path(output))){
            fileSystem.delete(new Path(output),true);
        }
        job.waitForCompletion(true);
    }
}
