package com.mr.day4.compress;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/21.
 */
public class TestReadCompressFile {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(value.toString());
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(TestReadCompressFile.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\data\\camera\\camera.tar.gz","");
    }
}


