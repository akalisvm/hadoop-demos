package com.tianang.mapreduce.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class ReduceJoinMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String filename;
    Text outKey = new Text();
    TableBean outValue = new TableBean();

    @Override
    protected void setup(Context context) {
        // get file name
        InputSplit split = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) split;
        filename = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // get one line
        String line = value.toString();

        // execute different behaviours according to the file
        String[] split = line.split("\t");
        if(filename.contains("order")) {
            outKey.set(split[1]);
            outValue.setId(split[0]);
            outValue.setPid(split[1]);
            outValue.setAmount(Integer.parseInt(split[2]));
            outValue.setPname("");
            outValue.setFlag("order");
        } else {
            outKey.set(split[0]);
            outValue.setId("");
            outValue.setPid(split[0]);
            outValue.setAmount(0);
            outValue.setPname(split[1]);
            outValue.setFlag("pd");
        }

        // write key and value
        context.write(outKey, outValue);
    }
}
