package com.mr.day2.partition;

import com.mr.day2.phone.Flow;
import com.oracle.util.JobUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by 代俊朴 on 2018/1/16.
 */
public class PhoneNumberForPartition {
    public static  class ForMapper  extends Mapper<LongWritable,Text, Flow,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs[] =value.toString().split("\t");
            Flow  flow=new Flow();
            flow.setUpFlow(Integer.parseInt(strs[3]));
            flow.setDownFlow(Integer.parseInt(strs[2]));
            flow.setAllFlow(Integer.parseInt(strs[1]));
            flow.setPhoneNumber(strs[0]);
            context.write(flow,NullWritable.get());
        }
    }
    public static class  ForReducer extends Reducer<Flow,NullWritable,Flow,NullWritable> {
        @Override
        protected void reduce(Flow key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
    public static class ForPartitioner extends Partitioner<Flow,NullWritable>{
        @Override
        public int getPartition(Flow flow, NullWritable nullWritable, int numPartitions) {
            if(flow.getPhoneNumber().substring(0,3).equals("136")){
                return 1;
            }else if(flow.getPhoneNumber().substring(0,3).equals("137")){
                return 2;
            }
            return 0;
        }
    }
   public static void main(String[] args) {
       JobUtil.commitJob(PhoneNumberForPartition.class,"E:\\\\OracleXLH\\\\大数据教学内容\\\\allOfSoft\\\\forTestData\\\\phoneFlow\\\\forSort","",new ForPartitioner(),3);
   }
}
