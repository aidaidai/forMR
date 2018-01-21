package com.mr.day4.forinput;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/21.
 */
public class TestKeyValueInput {
    public static class ForMapper extends Mapper<Text,Text,Text,Text>{
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            System.out.println(key.toString()+","+value.toString());
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(TestKeyValueInput.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\testkeyValue","",new KeyValueTextInputFormat());
    }
}
