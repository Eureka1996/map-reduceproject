package com.wufuqiang.mr.mappers;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/12/012 - 22:20
 **/
public class SequenceFileMapper extends Mapper<Text,BytesWritable,Text,BytesWritable> {
    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        System.out.println(key.toString());
        context.write(key,value);
    }
}
