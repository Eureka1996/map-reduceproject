package com.wufuqiang.mr.drivers;

import com.wufuqiang.mr.mappers.KVTextMapper;
import com.wufuqiang.mr.reducers.KVTextReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/11/011 - 21:44
 **/
public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration config = new Configuration();
        config.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(config);

        job.setJarByClass(KVTextDriver.class);
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);

        String inputPathStr = "D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\input\\input2.txt";
        String outputPathStr = "D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\output";
        FileInputFormat.setInputPaths(job,new Path(inputPathStr));
        FileOutputFormat.setOutputPath(job,new Path(outputPathStr));

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
