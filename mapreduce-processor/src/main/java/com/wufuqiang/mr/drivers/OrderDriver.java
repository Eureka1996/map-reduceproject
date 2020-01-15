package com.wufuqiang.mr.drivers;

import com.wufuqiang.mr.beans.OrderBean;
import com.wufuqiang.mr.mappers.OrderMapper;
import com.wufuqiang.mr.reducers.OrderReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class OrderDriver {
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(OrderDriver.class);
        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        String inputPathStr = "D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\input\\input1.txt";//args[0];
        String outputPathStr = "D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\output";//args[1];
        FileInputFormat.setInputPaths(job, new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));
    }
}
