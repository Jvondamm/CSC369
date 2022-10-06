import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class SecondarySortMapper
    extends Mapper<LongWritable, Text, YMTemperaturePair, IntWritable> {
  @Override
  public void map(LongWritable key, Text value, Context
     context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    String yearMonth = tokens[0].trim() +"-"+
                                       tokens[1].trim();
    int temperature = Integer.parseInt(tokens[3].trim());
    context.write(new YMTemperaturePair(yearMonth, temperature), new IntWritable(temperature));
    }
}
