package com.wufuqiang.mr.reducers;

import com.wufuqiang.mr.beans.OrderBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @ author wufuqiang
 **/
public class OrderReducer extends Reducer<OrderBean,NullWritable,OrderBean,NullWritable>{

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        context.write(key,NullWritable.get());
    }
}
