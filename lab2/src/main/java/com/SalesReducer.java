package main.java.com;

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;

public class SalesReducer
extends Reducer<Text, IntWritable, Text, DoubleWritable> {
   @Override
   public void reduce(Text date,
   Iterable<IntWritable> sales, Context context)
      throws IOException, InterruptedException {
      int count = 0;
      for(IntWritable sale: sales){
         count++;
      }
      context.write(date, new IntWritable(count));
   }
}