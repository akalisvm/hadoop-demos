package com.tianang.mapreduce.writablecompable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {

    FlowBean outKey = new FlowBean();
    Text outValue = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        String[] split = line.split("\t");

        String phone = split[1];
        String upFlow = split[split.length - 3];
        String downFlow = split[split.length - 2];

        outKey.setUpFlow(Long.parseLong(upFlow));
        outKey.setDownFlow(Long.parseLong(downFlow));
        outKey.setSumFlow();
        outValue.set(phone);

        context.write(outKey, outValue);
    }
}
