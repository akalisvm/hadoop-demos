package com.tianang.mapreduce.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setJarByClass(LogDriver.class);
        job.setMapperClass(LogMapper.class);
        job.setReducerClass(LogReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // Setting up the custom output format
        job.setOutputFormatClass(LogOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // Although there is a tailored output format, which extends fileoutputformat.
        // And fileoutputformat outputs a _SUCCESS file, thus we still need an output path.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        for(int i = 2; i < args.length; i++) {
            LogArgsList.getInstance().add(args[i]);
        }

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
