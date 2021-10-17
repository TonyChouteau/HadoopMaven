package org.apache.hadoop.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Count {
    public static void main(String[] args) throws IOException{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:9000"),conf);
        Path fullname = new Path("movies.csv");
        int nb_ligne = 0;

        FSDataInputStream inStream = fs.open(fullname);
        try {
            InputStreamReader isr = new InputStreamReader(inStream);
            BufferedReader br = new BufferedReader(isr);
            String line = br.readLine();
            while (line != null) {
                nb_ligne++;
                line = br.readLine();
            }
        }
        finally {inStream.close();fs.close();System.out.println(nb_ligne);}
    }
}
