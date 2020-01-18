package com.wufuqiang.mr.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ author wufuqiang
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TableBean implements Writable {
    private String id;
    private String pid;
    private int amount;
    private String pname;
    private String flag;

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);

    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.amount = dataInput.readInt();
        this.pname = dataInput.readUTF();
        this.flag = dataInput.readUTF();

    }

    @Override
    public String toString() {
        return id + "\t" + amount + "\t" + pname ;
    }
}
