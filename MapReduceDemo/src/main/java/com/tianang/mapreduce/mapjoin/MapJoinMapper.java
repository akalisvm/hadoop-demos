package com.tianang.mapreduce.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, Writable> {

    Map<String, String> pdMap = new HashMap<>();
    Text text = new Text();

    // add pd data to pdMap before starting the MapTask
    @Override
    protected void setup(Context context) throws IOException {

        // get small table data pd.txt through cached file
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);

        // get file system and open the stream
        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(path);

        // convert the stream to reader by wrapping it
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

        // read the file
        String line;
        while(StringUtils.isNotEmpty(line = reader.readLine())) {
            // split one line
            String[] split = line.split("\t");
            pdMap.put(split[0], split[1]);
        }

        // close the stream
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // read large table data
        String[] fields = value.toString().split("\t");

        // read pid from each line of large table
        String pname = pdMap.get(fields[1]);

        // pid replace to pname
        text.set(fields[0] + "\t" + pname + "\t" + fields[2]);

        // write key and value
        context.write(text, NullWritable.get());
    }
}
