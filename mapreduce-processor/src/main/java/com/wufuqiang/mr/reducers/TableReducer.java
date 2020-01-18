package com.wufuqiang.mr.reducers;

import com.wufuqiang.mr.beans.TableBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @ author wufuqiang
 **/
public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<TableBean> orderBeans = new ArrayList<TableBean>();
        TableBean pdBean = new TableBean();
        for(TableBean value : values){
            if("order".equals(value.getFlag())){
                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean,value);
                    orderBeans.add(tmpBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }else if("pd".equals(value.getFlag())){
                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean,value);
                    pdBean = tmpBean;
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }

            }
        }
        for(TableBean bean : orderBeans){
            bean.setPname(pdBean.getPname());
            context.write(bean,NullWritable.get());
        }
    }
}
