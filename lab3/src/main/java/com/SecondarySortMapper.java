import java.io.IOException;
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
    Sales Sale = new Sales(tokens[1], tokens[2], tokens[0]);
    Text Text = new Text(tokens[2] + ", " + tokens[0]);
    context.write(Sale, Text);
    }
}
