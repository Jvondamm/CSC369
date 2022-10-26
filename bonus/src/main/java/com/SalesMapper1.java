import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class SalesMapper1
    extends Mapper<LongWritable, Text, Sales, Text> {
  @Override
  public void map(LongWritable key, Text value, Context
     context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    IntWritable SalesID = new IntWritable(tokens[0].trim());
    IntWritable Date = new IntWritable(tokens[1].trim());
    IntWritable StoreID = new IntWritable(tokens[3].trim());
    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();
    outputKey.set(SalesID, new Text("2"));
    outputValue.set(new Text("S"), Date, StoreID);
    context.write(outputKey, outputValue);
    context.write();
    }
}