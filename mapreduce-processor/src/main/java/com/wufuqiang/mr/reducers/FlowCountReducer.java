package com.wufuqiang.mr.reducers;

import com.wufuqiang.mr.beans.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text,FlowBean> {

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long sumUpFlow = 0;
        long sumDownFlow = 0;
        for(FlowBean value : values){
            sumUpFlow += value.getUpFlow();
            sumDownFlow += value.getDownFlow();
        }
        FlowBean sumFlowBean = new FlowBean(sumUpFlow,sumDownFlow);
        context.write(key,sumFlowBean);
    }
}
