import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.*;
import org.apache.log4j.Logger;

public class SecondarySortMapper
    extends Mapper<LongWritable, Text, Sales, Text> {
  @Override
  public void map(LongWritable key, Text value, Context
     context) throws IOException, InterruptedException {
    String line = value.toString();
    String[] tokens = line.split(",");
    String obj1 = new Sale(tokens[1], tokens[2], tokens[0]);
    String obj2 = new Text(tokens[2] + ", " + tokens[0]);
    context.write(obj1, obj2);
    }
}
