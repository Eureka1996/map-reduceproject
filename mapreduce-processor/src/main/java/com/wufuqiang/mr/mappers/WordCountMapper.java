package com.wufuqiang.mr.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        super.map(key, value, context);
        String line = value.toString();
        String[] words = line.split(" ");
        Text outputKey = new Text();
        IntWritable outputValue = new IntWritable(1);
        for(String word : words){
            outputKey.set(word);
            context.write(outputKey,outputValue);
        }
    }
}