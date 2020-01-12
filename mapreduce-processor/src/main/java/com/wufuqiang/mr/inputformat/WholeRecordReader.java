package com.wufuqiang.mr.inputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/12/012 - 21:46
 **/
public class WholeRecordReader extends RecordReader<Text,BytesWritable> {

    FileSplit split;
    Configuration config;
    Text k = new Text();
    BytesWritable v = new BytesWritable() ;
    boolean isProgress = true;

    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        //将InputSplit转为F
        split = (FileSplit)inputSplit;
        config = taskAttemptContext.getConfiguration();
    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        //核心业务逻辑处理
        if(isProgress){
            byte[] buf = new byte[(int) split.getLength()];
            //获取fs对象
            Path path = split.getPath();
            FileSystem fs = path.getFileSystem(config);
            FSDataInputStream fis = fs.open(path);
            IOUtils.readFully(fis,buf,0,buf.length);
            k.set(path.toString());
            System.out.println("这是Path："+path.toString());
            v.set(buf,0,buf.length);
            IOUtils.closeStream(fis);
            isProgress = false;
            return true;
        }

        return false;
    }

    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return v;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
