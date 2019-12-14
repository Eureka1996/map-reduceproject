package com.wufuqiang.mr.reducers;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/12/012 - 22:24
 **/
public class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        for(BytesWritable value : values){
            System.out.println(value);
            context.write(key,value);
        }
    }
}
