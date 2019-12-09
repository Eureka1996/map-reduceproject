package com.wufuqiang.mr.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable, Text,IntWritable> {
    private IntWritable value = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum  =0 ;
        for(IntWritable value : values){
            int count = value.get();
            sum+=count;
        }
        value.set(sum);
        context.write(key,value);
    }
}
