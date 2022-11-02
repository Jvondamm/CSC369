import java.io.IOException;
import java.io.*;
import java.util.TreeSet;
import java.util.Arrays;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class TopNMapper4
    extends Mapper<LongWritable, Text, PairOfStrings, PairOfStrings> {

    public static final int DEFAULT_N = 10;
    private int n = DEFAULT_N;

    @Override
    public void map(LongWritable key, Text value, Context
      context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] tokens = line.split(",");
      Text Date = new Text(tokens[0].trim());
      Text Values = new Text(Date+" "+tokens[1].trim()+" "+
      tokens[2].trim()+" "+tokens[3].trim());
      PairOfStrings outputKey = new PairOfStrings();
      PairOfStrings outputValue = new PairOfStrings();
      outputKey.set(Date, new Text("1"));
      outputValue.set(new Text("1"), Values);
      context.write(outputKey, outputValue);
    }
}