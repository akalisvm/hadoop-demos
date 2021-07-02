package com.tianang.mapreduce.weblog;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WebLogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // get one line
        String line = value.toString();

        // parse log
        boolean result = parseLog(line);

        // write the satisfied log out
        if(result) {
            context.write(value, NullWritable.get());
        }
    }

    private boolean parseLog(String line) {

        String[] fields = line.split(" ");

        return fields.length > 11;
    }

}
