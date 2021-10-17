package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class DisplayFolder {
    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        FileStatus[] fileStatus = fs.listStatus(new Path("/user/movies/"));
        for(FileStatus status : fileStatus){
            Path fullname=new Path(status.getPath().toString());
            System.out.println("Path: "+fullname);
            FSDataInputStream inStream = fs.open(fullname);
            try {
                InputStreamReader isr = new InputStreamReader(inStream);
                BufferedReader br = new BufferedReader(isr);
                String line = br.readLine();
                while (line != null){
                    System.out.println(line);
                    line = br.readLine();
                }
            }
            finally {inStream.close();}
        }
        fs.close();
    }
}