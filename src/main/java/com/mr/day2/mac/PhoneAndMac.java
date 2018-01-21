package com.mr.day2.mac;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 代俊朴 on 2018/1/16.
 */
public class PhoneAndMac {
    public static  class ForMapper extends Mapper<LongWritable,Text,Text,MacFlow>{
        MacFlow macFlow=new MacFlow();
        Text okey=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              String str [] =value.toString().split("\t");
              macFlow.setDownFlow(Integer.parseInt(str[8]));
              macFlow.setUpFlow(Integer.parseInt(str[9]));
              macFlow.setAllFlow(macFlow.getAllFlow());
              macFlow.setMac(str[2]);
              okey.set(str[1]);
              context.write(okey,macFlow);
        }
    }
    public static class ForReducer extends Reducer<Text,MacFlow,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<MacFlow> values, Context context) throws IOException, InterruptedException {
              Map<String,Integer> map=new HashMap<>();//用于保存相同的mac和对应的流量
              for(MacFlow macFlow:values){
                 if(map.containsKey(macFlow.getMac())){
                     map.put(macFlow.getMac(),map.get(macFlow.getMac())+macFlow.getAllFlow());
                 }else {
                     map.put(macFlow.getMac(),macFlow.getAllFlow());
                 }
              }
              context.write(key,new Text(map.toString()));
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(PhoneAndMac.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\phoneFlow\\data","");
    }
}
