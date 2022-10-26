import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class ProductMapper1
    extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
  @Override
  public void map(LongWritable key, Text value, Context
     context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    IntWritable ProductID = new IntWritable(tokens[0].trim());
    Text Price = new Text(tokens[2].trim());
    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();
    outputKey.set(ProductID, new Text("2"));
    outputValue.set(new Text("P"), Price);
    context.write(outputKey, outputValue);
    context.write();
    }
}