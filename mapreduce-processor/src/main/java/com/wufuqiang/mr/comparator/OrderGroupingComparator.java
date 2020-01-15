package com.wufuqiang.mr.comparator;

import com.wufuqiang.mr.beans.OrderBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @ author wufuqiang
 **/
public class OrderGroupingComparator extends WritableComparator {
    public OrderGroupingComparator() {
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean aBean = (OrderBean)a;
        OrderBean bBean = (OrderBean)b;
        int result=0;
        if(aBean.getOrderId()>bBean.getOrderId()){
            result = 1;
        }else if(aBean.getOrderId() < bBean.getOrderId()){
            result = -1;
        }else{
            result = 0;
        }
        return result;
    }
}
