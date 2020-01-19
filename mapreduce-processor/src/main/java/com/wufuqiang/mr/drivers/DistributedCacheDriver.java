package com.wufuqiang.mr.drivers;

import com.wufuqiang.mr.mappers.DistributedCacheMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @ author wufuqiang
 **/
public class DistributedCacheDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(DistributedCacheDriver.class);

        job.setMapperClass(DistributedCacheMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path(""));
        FileOutputFormat.setOutputPath(job,new Path(""));


        job.setCacheFiles(new URI[]{new URI("file:///")});
        job.setNumReduceTasks(0);

        boolean result = job.waitForCompletion(true);
        System.exit(result?0:1);
    }
}
