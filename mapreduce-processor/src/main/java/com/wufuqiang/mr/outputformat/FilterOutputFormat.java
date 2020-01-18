package com.wufuqiang.mr.outputformat;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class FilterOutputFormat extends FileOutputFormat<Text,NullWritable> {

    public RecordWriter<Text, NullWritable> getRecordWriter(FileSystem fileSystem, JobConf jobConf, String s, Progressable progressable) throws IOException {
        return new FilterRecordWriter(fileSystem);
    }
}
