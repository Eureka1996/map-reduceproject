package com.wufuqiang.mr.drivers;

import com.wufuqiang.mr.beans.FlowBean;
import com.wufuqiang.mr.mappers.FlowCountMapper;
import com.wufuqiang.mr.reducers.FlowCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowCountDriver.class);
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        String inputPathStr = "/Users/wufuqiang/IdeaProjects/map-reduceproject/mapreduce-processor/src/main/resources/input/phone-data.txt";//args[0];
        String outputPathStr = "/Users/wufuqiang/IdeaProjects/map-reduceproject/mapreduce-processor/src/main/resources/output";//args[1];
        FileInputFormat.setInputPaths(job,new Path(inputPathStr));
        FileOutputFormat.setOutputPath(job,new Path(outputPathStr));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);


    }
}
