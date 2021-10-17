package org.apache.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class DisplayFile {
    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        Path fullname = new Path("new");
        FSDataInputStream inStream = fs.open(fullname);
        try {
            InputStreamReader isr = new InputStreamReader(inStream);
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();
            while (line != null) {
                try{System.out.println(line);}
                catch(Exception e){}
                line = br.readLine();
            }
        }
        finally {inStream.close();fs.close();}
    }
}