package com.wufuqiang.mr.mappers;

import com.wufuqiang.mr.beans.FlowBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text,Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] lineSplited = line.split(" ");
        Text outputKey = new Text(lineSplited[0]);
        FlowBean outputValue = new FlowBean(Long.parseLong(lineSplited[1]),Long.parseLong(lineSplited[2]));
        context.write(outputKey,outputValue);
    }
}
