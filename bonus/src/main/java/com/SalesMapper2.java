import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class SalesMapper2
    extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
  @Override
  public void map(LongWritable key, Text value, Context
     context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    Text SalesID = new Text(tokens[0].trim());
    String[] date = tokens[1].split("/");
    Text Values = new Text(date[0]+"-"+date[1]+" "+tokens[3]);
    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();
    outputKey.set(SalesID, new Text("1"));
    outputValue.set(new Text("S"), Values);
    context.write(outputKey, outputValue);
    }
}