package com.wufuqiang.mr.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/11/011 - 21:39
 **/
public class KVTextReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable resultCount = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for(IntWritable value : values){
            int i = value.get();
            count+=i;
        }

        resultCount.set(count);
        context.write(key,resultCount);
    }
}
