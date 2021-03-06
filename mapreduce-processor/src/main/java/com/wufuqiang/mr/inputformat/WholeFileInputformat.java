package com.wufuqiang.mr.inputformat;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @ author wufuqiang
 * @ date 2019/12/12/012 - 21:40
 **/
public class WholeFileInputformat extends FileInputFormat<Text,BytesWritable> {

    /**
     * 重写createRecordReader()，创建自定义的RecordReader对象，并初始化
     * @param inputSplit
     * @param taskAttemptContext
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeRecordReader recordReader = new WholeRecordReader();
        recordReader.initialize(inputSplit,taskAttemptContext);
        return recordReader;
    }
}
