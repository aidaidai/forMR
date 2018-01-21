package com.mr.day3.join;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by 代俊朴 on 2018/1/18.
 */
public class MapJoinMR {
    public static class  ForMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        private Map<String,String> cache=new HashMap<>();
        private Text ovalue=new Text();
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
           URI uris[]= context.getCacheFiles();
           //读取该文件的内容
            BufferedReader bufferedReader=new BufferedReader(new FileReader(new File(uris[0])));
            String temp;
            //装入缓存集合
            while ((temp=bufferedReader.readLine())!=null){
                 String str [] =temp.split("\t");
                 cache.put(str[0],str[1]);
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
              //读取缓存 与大文件进行合并
            String line=value.toString();
            String str [] =line.split("\t");
            StringBuilder  sb=new StringBuilder();//准备拼接最后的结果
            sb.append(str[0]+"-");//编号
            sb.append(str[1]+"-");//姓名
            String temp=cache.get(str[2]);//从缓存里找对应键（mac）的值（手机型号）
            sb.append(temp);//拼接成
            ovalue.set(sb.toString());
            context.write(ovalue,NullWritable.get());
        }
    }
    public static void main(String[] args) throws URISyntaxException {
        JobUtil.commitJob(MapJoinMR.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\jionData\\map\\userinfo.txt","",
                new URI("file:///E:/OracleXLH/大数据教学内容/allOfSoft/forTestData/jionData/map/phoneinfo.txt"));
    }
}
