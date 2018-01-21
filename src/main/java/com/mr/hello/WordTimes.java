package com.mr.hello;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by 代俊朴 on 2018/1/15.
 */
public class WordTimes implements WritableComparable<WordTimes>{
     private String word;
     private int num;
    public WordTimes(String word, int num) {
        this.word = word;
        this.num = num;
    }
    public WordTimes() {
    }
    @Override
    public int compareTo(WordTimes o) {

        return o.num-this.num;
    }

    @Override
    public void write(DataOutput out) throws IOException {
            out.writeInt(num);
            out.writeUTF(word);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
            this.num=in.readInt();
            this.word=in.readUTF();
    }

    @Override
    public String toString() {
        return "WordTimes{" +
                "word='" + word + '\'' +
                ", num=" + num +
                '}';
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public static void main(String[] args) {
        WordTimes wt=new WordTimes("a",1);
        WordTimes wt1=new WordTimes("b",10);
        WordTimes wt2=new WordTimes("c",12);
        WordTimes wt3=new WordTimes("d",5);
        List<WordTimes> list=new ArrayList<>();
        list.add(wt);list.add(wt1);list.add(wt2);list.add(null);
        Collections.sort(list);
        list.remove(3);
        System.out.println(list);
    }
}
