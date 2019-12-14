package com.wufuqiang.mr.drivers;

import com.wufuqiang.mr.inputformat.WholeFileInputformat;
import com.wufuqiang.mr.mappers.SequenceFileMapper;
import com.wufuqiang.mr.reducers.SequenceFileReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/12/012 - 22:27
 **/
public class SequenceFileDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\input2\\",
                "D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\output"};
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        job.setInputFormatClass(WholeFileInputformat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.out.println(result);
        System.exit(result?0:1);
    }
}
