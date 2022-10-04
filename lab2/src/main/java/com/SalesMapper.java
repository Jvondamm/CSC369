

import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class SalesMapper extends
         Mapper<LongWritable, Text, Text, IntWritable> {
   private static final Logger THE_LOGGER =
      Logger.getLogger(SalesMapper.class);

   @Override
   public void map(LongWritable key, Text value, Context context)
         throws IOException, InterruptedException {
      String valueAsString = value.toString().trim();
      String[] tokens = valueAsString.split(" ");

      THE_LOGGER.info(tokens, tokens.length);

      context.write(new Text(tokens[1]),
                  new IntWritable(1));
   }
}