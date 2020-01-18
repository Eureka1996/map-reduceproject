package com.wufuqiang.mr.mappers;

import com.wufuqiang.mr.beans.TableBean;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean> {
    private String name;
    private TableBean tableBean = new TableBean();
    Text k = new Text();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //获取文件名
        FileSplit inputSplit = (FileSplit)context.getInputSplit();
        name = inputSplit.getPath().getName();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if(name.startsWith("order")){
            String[] fields = line.split("\t");
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setPname("");
            tableBean.setFlag("order");
            k.set(fields[1]);
        }else{
            String[] fields = line.split("\t");
            tableBean.setId("");
            tableBean.setPid(fields[0]);
            tableBean.setAmount(0);
            tableBean.setPname(fields[1]);
            tableBean.setFlag("pd");
            k.set(fields[0]);

        }
        context.write(k,tableBean);
    }
}
