package com.mr.day3.join;

import com.oracle.util.JobUtil;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试 在reduce端的join
 * Created by 代俊朴 on 2018/1/18.
 */
public class ReduceJoinMR {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,Info>{
        private Text okey=new Text();
        private Info info=new Info();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line =value.toString();
            String str [] =line.split("\t");
            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            String filename=fileSplit.getPath().getName();
            if(str.length<2){
                return ;
            }
            if("order.txt".equals(filename)){
                info.setFlag(false);//订单
                String pid=str[3];//三号位是pid
                okey.set(pid);//设置key
                info.setFlag(false);//表明该实体是order
                info.setOrderId(str[0]);
                info.setAmount(Integer.parseInt(str[2]));
                info.setDateTime(str[1]);
                info.setpId(str[3]);
                info.setpName("");
                info.setPrice(0);
                context.write(okey,info);
            }else if("product.txt".equals(filename)){
                info.setFlag(true);// 产品
                okey.set(str[0]);//因为是product表所以0号位置是pid
                info.setpId(str[0]);
                info.setpName(str[1]);
                info.setPrice(Integer.parseInt(str[3]));
                context.write(okey,info);
            }else{
                System.err.println("error ...........");
            }
        }
    }
    public static class ForReducer extends Reducer<Text,Info,Info,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Info> values, Context context) throws IOException, InterruptedException {
            Info pd=new Info();//每个商品
            List<Info> orderList=new ArrayList<>();//每个商品对应的order list  每个key的最后的结果集合
            for(Info  info:values){
                try {
                    if(info.isFlag())//如果是商品
                        BeanUtils.copyProperties(pd,info); //将商品的属性取出来
                    else {  //如果是订单，注意订单有可能有多个
                        Info order = new Info();
                        BeanUtils.copyProperties(order, info);//将订单信息从info中copy出来
                        orderList.add(order);
                    }
                }
                catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
            //组织最后的结果，为每个订单设置对应的商品
            for(Info bean :orderList){
                bean.setpName(pd.getpName());
                bean.setpId(pd.getpId());
                bean.setPrice(pd.getPrice());
                context.write(bean,NullWritable.get());
            }
            //组织最后的结果
        }
    }
    public static void main(String[] args) {
        JobUtil.commitJob(ReduceJoinMR.class,"E:\\OracleXLH\\大数据教学内容\\allOfSoft\\forTestData\\jionData\\reduce","");
    }

}

