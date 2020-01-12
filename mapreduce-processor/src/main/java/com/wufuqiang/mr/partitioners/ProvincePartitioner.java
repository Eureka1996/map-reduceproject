package com.wufuqiang.mr.partitioners;

import com.wufuqiang.mr.beans.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @ author wufuqiang
 **/
public class ProvincePartitioner extends Partitioner<Text,FlowBean>{

    public int getPartition(Text key, FlowBean value, int i) {

        int partition = 4;
        String phone = key.toString();
        if(phone.startsWith("136")){
            partition = 0;
        }else if(phone.startsWith("137")){
            partition = 1;
        }else if(phone.startsWith("138")){
            partition = 2;
        }else if(phone.startsWith("139")){
            partition = 3;
        }
        return partition;
    }
}
