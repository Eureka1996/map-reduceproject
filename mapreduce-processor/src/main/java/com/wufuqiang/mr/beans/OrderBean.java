package com.wufuqiang.mr.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ author wufuqiang
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;
    private double price;

    public int compareTo(OrderBean o) {
        if(this.orderId > o.orderId){
            return 1;
        }else if(this.orderId < o.orderId){
            return -1;
        }else{
            if(this.price > o.price){
                return -1;
            }else if(this.price < o.price){
                return 1;
            }else{
                return 0;
            }
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(orderId);
        dataOutput.writeDouble(price);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.orderId = dataInput.readInt();
        this.price = dataInput.readDouble();
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
