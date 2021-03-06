package com.tianang.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text outKey = new Text();
    IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. Getting one line
        String line = value.toString();

        // 2. Splitting words
        String[] words = line.split(" ");

        // 3. Output
        for(String word : words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }
}
