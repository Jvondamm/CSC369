import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class StoreMapper3
    extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {
  @Override
  public void map(LongWritable key, Text value, Context
     context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    Text StoreID = new Text(tokens[0].trim());
    Text Values = new Text(tokens[1].trim()+" "+tokens[3].trim());
    PairOfStrings outputKey = new PairOfStrings();
    PairOfStrings outputValue = new PairOfStrings();
    outputKey.set(StoreID, new Text("1"));
    outputValue.set(new Text("ST"), Values);
    context.write(outputKey, outputValue);
    }
}