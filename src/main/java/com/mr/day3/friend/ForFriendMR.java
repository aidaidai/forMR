package com.mr.day3.friend;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 *  找粉丝
 * Created by 代俊朴 on 2018/1/18.
 */
public class ForFriendMR {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Text>{
        private Text okey=new Text();
        private Text ovalue=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String strs[]=value.toString().split(":");// 0 是 person  1 是他的所有粉丝
             ovalue.set(strs[0]);
             String friends [] = strs[1].split(",");
             for(String s:friends){
                 okey.set(s);
                 context.write(okey,ovalue); //将person当做值 将friend当做键 送到reduce 端重新 混洗
             }
        }
    }
    public static class ForReducer extends Reducer<Text,Text,Text,Text> {
        private Text  ovalue =new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
             // 接收到的键是每个用户  而值是该用户关注了的人
             StringBuilder sb=new StringBuilder();
             for(Text text:values){
                 sb.append(text.toString()+",");
             }
             ovalue.set(sb.toString());
             context.write(key,ovalue);
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(ForFriendMR.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\friend","");
    }
}



















