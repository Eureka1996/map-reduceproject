package com.wufuqiang.mr.outputformat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
@Data
@NoArgsConstructor
public class FilterRecordWriter implements RecordWriter<Text, NullWritable> {
    private FileSystem fileSystem;
    private JobConf jobConf;
    private FSDataOutputStream fos1;
    private FSDataOutputStream fos2;


    public FilterRecordWriter(FileSystem fileSystem,JobConf jobConf) throws IOException {
        this.fileSystem = fileSystem;
        this.jobConf = jobConf;
        fos1 = fileSystem.create(new Path("D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\output\\file1"));
        fos2 = fileSystem.create(new Path("D:\\wufuqiangbd\\ideaProjects\\map-reduceproject\\mapreduce-processor\\src\\main\\resources\\output\\file2"));
    }

    public void write(Text key, NullWritable nullWritable) throws IOException {
        if(key.toString().contains("xiaomi")){
            fos1.write(key.toString().getBytes());
        }else{
            fos2.write(key.toString().getBytes());
        }

    }

    public void close(Reporter reporter) throws IOException {
        IOUtils.closeStream(fos1);
        IOUtils.closeStream(fos2);
    }
}
