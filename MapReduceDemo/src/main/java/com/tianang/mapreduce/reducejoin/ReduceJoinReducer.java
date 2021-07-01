package com.tianang.mapreduce.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class ReduceJoinReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {

        ArrayList<TableBean> orderBeans = new ArrayList<>();
        TableBean pdBean = new TableBean();

        for(TableBean value : values) {

            // determine which table the data come from
            if("order".equals(value.getFlag())) { // order table

                // create a temporary TableBean to receive value
                TableBean tmpOrderBean = new TableBean();

                try {
                    BeanUtils.copyProperties(tmpOrderBean, value);
                } catch (InvocationTargetException | IllegalAccessException e) {
                    e.printStackTrace();
                }

                // add temporary TableBean to the collection orderBeans
                orderBeans.add(tmpOrderBean);
            } else { // pd table
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (InvocationTargetException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }

        // transverse the collection orderBeans, each pid replace to pname, and then write it out
        for(TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());
            // write orderBean after modified
            context.write(orderBean, NullWritable.get());
        }
    }
}
