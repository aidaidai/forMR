package com.mr.day2.phone;

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
 * Created by 代俊朴 on 2018/1/16.
 */
public class PhoneMacFlowTopOne {
     public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
         Text  okey=new Text();
         Text  ovalue=new Text();
         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                String  line=value.toString();
                String str [] = line.split("\t");
                okey.set(str[0]);//手机号
                    String[] macFlows = str[1].substring(1,str[1].length()-1).split(",");
                    int max = 0;
                    String macLast = "";
                    for (String macFlow : macFlows) {
                        String mac = macFlow.split("=")[0];
                        int flow = Integer.parseInt(macFlow.split("=")[1]);
                        if (flow > max) {
                            max = flow;
                            macLast = mac;
                        }

                    }
                    ovalue.set(macLast + "," + max);
                    context.write(okey, ovalue);
                }

     }

    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        String input="E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\phoneFlow\\macFlowSort";
        String output="E:/output/";
        FileInputFormat.setInputPaths(job,new Path(input));
        FileSystem fileSystem=FileSystem.get(new URI("file://"+output),new Configuration());
        if(fileSystem.exists(new Path(output))) {
            fileSystem.delete(new Path(output), true);
        }
        FileOutputFormat.setOutputPath(job,new Path(output)); //输出路径
        job.waitForCompletion(true);
    }
}
