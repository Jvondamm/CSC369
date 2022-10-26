import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class LineItemMapper1
    extends Mapper<LongWritable, Text, PairOfStrings, PairOfStringsAndInt> {
  @Override
  public void map(LongWritable key, Text value, Context
     context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    IntWritable SalesID = new IntWritable(tokens[1].trim());
    IntWritable ProductID = new IntWritable(tokens[2].trim());
    IntWritable Quantity = new IntWritable(tokens[3].trim());
    PairOfStrings outputKey = new PairOfStrings();
    PairOfStringsAndInt outputValue = new PairOfStringsAndInt();
    outputKey.set(ProductID, new Text("1"));
    outputValue.set(new Text("L"), SalesID, Quantity);
    context.write(outputKey, outputValue);
    context.write();
    }
}