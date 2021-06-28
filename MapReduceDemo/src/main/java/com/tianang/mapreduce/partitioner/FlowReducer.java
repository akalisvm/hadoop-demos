package com.tianang.mapreduce.partitioner;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean outValue = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {

        long totalUpFlow = 0;
        long totalDownFlow = 0;

        for(FlowBean flowBean : values) {
            totalUpFlow += flowBean.getUpFlow();
            totalDownFlow += flowBean.getDownFlow();
        }

        outValue.setUpFlow(totalUpFlow);
        outValue.setDownFlow(totalDownFlow);
        outValue.setSumFlow();

        context.write(key, outValue);
    }
}

