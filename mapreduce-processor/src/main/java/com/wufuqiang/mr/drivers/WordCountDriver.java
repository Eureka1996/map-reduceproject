package com.wufuqiang.mr.drivers;


import com.wufuqiang.mr.mappers.WordCountMapper;
import com.wufuqiang.mr.reducers.WordCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {

        //获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(WordCountDriver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(CombineTextInputFormat.class);
        CombineTextInputFormat.setMaxInputSplitSize(job,20971520);

        job.setNumReduceTasks(2);

        String inputPathStr = "D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\input\\input1.txt";//args[0];
        String outputPathStr = "D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\output";//args[1];
        FileInputFormat.setInputPaths(job,new Path(inputPathStr));
        FileOutputFormat.setOutputPath(job,new Path(outputPathStr));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
