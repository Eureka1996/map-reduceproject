package com.wufuqiang.mr.mappers;

import com.wufuqiang.mr.beans.OrderBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class OrderMapper extends Mapper<LongWritable,Text,OrderBean,NullWritable> {
    OrderBean k = new OrderBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(" ");
        k.setOrderId(Integer.parseInt(fields[0]));
        k.setPrice(Double.parseDouble(fields[1]));
        context.write(k,NullWritable.get());
    }
}
