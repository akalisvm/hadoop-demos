package com.tianang.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

public class HDFSClient {

    private FileSystem fs;

    @Before
    public void init() throws IOException, URISyntaxException, InterruptedException {
        Configuration configuration = new Configuration();
        fs = FileSystem.get(new URI("hdfs://hadoop102:8020"), configuration, "tianang");
    }

    @After
    public void close() throws IOException {
        fs.close();
    }

    @Test
    public void testMkdirs() throws IOException {
        /*
         * usage: make a new directory
         * argument: the HDFS path to be created
         */
        fs.mkdirs(new Path("/DeathBoyAndBlackMaid/Alice"));
    }

    @Test
    public void testPut() throws IOException {
        /*
         * usage: copy the file from local to HDFS
         * arguments:
         * 1. whether delete the source file or not
         * 2. whether overwrite the existed file or not
         * 3. the local source path
         * 4. the HDFS destination path
         */
        fs.copyFromLocalFile(false, true, new Path("F:/Hadoop/HDFSClient/tmp/Bohemian.txt"),
                new Path("/DeathBoyAndBlackMaid/Alice"));
    }

    @Test
    public void testGet() throws IOException {
        /*
         * usage: copy the file from HDFS to local
         * arguments:
         * 1. whether delete the source file or not
         * 2. the HDFS source path
         * 3. the local destination path
         * 4. whether check the integrity of the file
         */
        fs.copyToLocalFile(false, new Path("/DeathBoyAndBlackMaid/Alice"),
                new Path("F:/Hadoop/HDFSClient/tmp/Bohemian.txt"), true);
    }

    @Test
    public void testRename() throws IOException {
        /*
         * usage: rename the name of file
         * arguments:
         * 1. the original path name
         * 2. the targeted path name
         */
        fs.rename(new Path("/DeathBoyAndBlackMaid/Alice/Bohemian.txt"),
                new Path("/DeathBoyAndBlackMaid/Alice/Mimi.txt"));
    }

    @Test
    public void testDelete() throws IOException {
        /*
         * usage: delete the directory
         * arguments:
         * 1. the path name
         * 2. whether list all files recursively
         */
        fs.delete(new Path("/DeathBoyAndBlackMaid"), true);
    }

    @Test
    public void testListFiles() throws IOException {
        /*
         * usage: list the information of files
         * arguments:
         * 1. the path name
         * 2. whether list all files recursively
         */
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while(listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();

            System.out.println("========" + fileStatus.getPath() + "=========");
            System.out.println(fileStatus.getPermission());
            System.out.println(fileStatus.getOwner());
            System.out.println(fileStatus.getGroup());
            System.out.println(fileStatus.getLen());
            System.out.println(fileStatus.getModificationTime());
            System.out.println(fileStatus.getReplication());
            System.out.println(fileStatus.getBlockSize());
            System.out.println(fileStatus.getPath().getName());

            // get block info
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }
    }

    @Test
    public void testListStatus() throws IOException {
        /*
         * usage: list the status of files
         * arguments:
         * 1. the path name
         * 2. whether list all files recursively
         */
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for(FileStatus fileStatus : listStatus) {
            if(fileStatus.isFile()) {
                System.out.println("f: " + fileStatus.getPath().getName());
            } else {
                System.out.println("d: " + fileStatus.getPath().getName());
            }
        }
    }
}
