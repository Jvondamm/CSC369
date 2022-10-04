

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;

public class SalesReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {
   private static final Logger THE_LOGGER =
      Logger.getLogger(SalesReducer.class);

   @Override
   public void reduce(Text date,
   Iterable<IntWritable> sales, Context context)
      throws IOException, InterruptedException {
      int count = 0;
      for(IntWritable sale: sales){
         count = count + sale;
      }
      context.write(date, new IntWritable(count));
   }
}