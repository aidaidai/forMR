package com.mr.day3.combiner;

import com.oracle.util.JobUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.net.URISyntaxException;

/**
 * Created by 代俊朴 on 2017/9/29.
 */
public class TemperatureAvg {
    public static class ForMapper extends Mapper<LongWritable, Text, Text, AvgEntity> {
        private Text okey=new Text();
        private AvgEntity avgEntity=new AvgEntity();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String strs[] = value.toString().split(" ");
            okey.set(strs[0]);
            avgEntity.setSum(Integer.parseInt(strs[1]));
            context.write(new Text(strs[0]), avgEntity);
        }
    }
    public static class AvgCombiner extends Reducer<Text, AvgEntity, Text, AvgEntity> {
        private AvgEntity ovalue=new AvgEntity();
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (AvgEntity iw : values) {
                sum += iw.getSum();
                count++;
            }
            ovalue.setSum(sum);
            ovalue.setCount(count);
            context.write(key, ovalue);

        }
    }
    public static class ForReducer extends Reducer<Text, AvgEntity, Text, DoubleWritable> {
        private DoubleWritable ovalue=new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            double count = 0;
            for (AvgEntity avgEntity : values) {
                sum += avgEntity.getSum();
                count += avgEntity.getCount();
            }
            ovalue.set(sum/count);
            context.write(key, ovalue);
        }
    }
    public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
        JobUtil.commitJob(TemperatureAvg.class, "E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\forCombiner", "");
    }
}