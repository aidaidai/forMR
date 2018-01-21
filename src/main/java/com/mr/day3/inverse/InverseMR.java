package com.mr.day3.inverse;
import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
/**
 * Created by 代俊朴 on 2018/1/18.
 */
public class InverseMR  {
     public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
         private Text okey =new Text();
         private Text ovalue=new Text();
         @Override
         protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             FileSplit fileSplit= (FileSplit) context.getInputSplit();
             String filename=fileSplit.getPath().getName();//文件名
             String line=value.toString();
             String strs [] = line.split(" ");//按照空格切分
             for(String s:strs){
                 okey.set(s+" "+filename);
                 context.write(okey,ovalue);
             }
         }
     }
     public static  class ForCombiner extends Reducer<Text,Text,Text,Text>{
         private Text ovalue =new Text();
         private Text okey=new Text();
         @Override
         protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
             int  count=0;
             String strs [] =key.toString().split(" ");
             String forKey=strs[0];
             String forFile=strs[1];
              for(Text text:values){
                  count++;
              }
              okey.set(forKey);
              ovalue.set(forFile+"-->"+count);
              context.write(okey,ovalue);
         }
     }
     public static class ForReducer extends  Reducer<Text,Text,Text,Text>{
         private Text ovalue =new Text ();
         @Override
         protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                StringBuilder sb=new StringBuilder();
                for(Text text :values ){
                    sb.append(text+"\t");
                }
                ovalue.set(sb.toString());
                context.write(key,ovalue);
         }
     }
    public static void main(String[] args) {
        JobUtil.commitJob(InverseMR.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\inverseIndex","",new ForCombiner());
    }
}















