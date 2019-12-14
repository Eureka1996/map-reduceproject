package com.wufuqiang.mr.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/11/011 - 21:31
 **/
public class KVTextMapper extends Mapper<Text, Text, Text, IntWritable> {

    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(key.toString());
        context.write(key,intWritable);
    }
}
