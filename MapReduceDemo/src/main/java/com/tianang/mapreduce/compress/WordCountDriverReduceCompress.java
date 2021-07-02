package com.tianang.mapreduce.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriverReduceCompress {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1. Getting configuration and jar object.
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2. Setting up jar object.
        job.setJarByClass(WordCountDriverMapCompress.class);

        // 3. Setting up mapper jar and reducer jar.
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. Setting up the classes of Mapper output keys and values.
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. Setting up the classes of final output keys and values.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. Setting up the input path and output path.
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Compressing at reduce task.
        FileOutputFormat.setCompressOutput(job, true);

        //Setting up the approach to compress.
        FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
        // FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
        // FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        // 7. Submitting the job.
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

