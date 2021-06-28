package com.tianang.mapreduce.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {

    private FSDataOutputStream atguiguOut;
    private FSDataOutputStream otherOut;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            // get file system object
            FileSystem fs = FileSystem.get(job.getConfiguration());
            // use file system object creates two output streams which corresponds two different directories
            atguiguOut = fs.create(new Path(LogArgsList.getInstance().get(0)));
            otherOut = fs.create(new Path(LogArgsList.getInstance().get(1)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        String log = key.toString();
        // determine the contents of two streams according to whether this line contains "atguigu"
        if(log.contains("atguigu")) {
            atguiguOut.writeBytes(log + "\n");
        } else {
            otherOut.writeBytes(log + "\n");
        }
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // close the streams
        IOUtils.closeStream(atguiguOut);
        IOUtils.closeStream(otherOut);
    }
}
