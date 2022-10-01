package main.java.com;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;

public class SalesMapper extends
         Mapper<LongWritable, Text, Text, IntWritable> {
   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
      String valueAsString = value.toString().trim();
      String[] tokens = valueAsString.split(" ");
      if (tokens.length != 5) {
         return;
      }
      context.write(new Text(tokens[1]),
                  new IntWritable(1));
   }
}