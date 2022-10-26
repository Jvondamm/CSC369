import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;

public class Reducer1
        extends Reducer<PairOfStrings, PairOfStrings, Text, Text> {
    IntWritable Price = new IntWritable();
    IntWritable Quantity = new IntWritable();
    IntWritable SalesID = new IntWritable();
    Text Name = new Text();
    @Override
    public void reduce(PairOfStrings key,
                   Iterable<PairOfStrings> values, Context context)
       throws java.io.IOException, InterruptedException {
       Iterator<PairOfStrings> iterator = values.iterator();
       PairOfStrings firstPair = iterator.next();
       if (firstPair.getLeftElement().toString().equals("L")) {
          String[] tokens = firstPair.getRightElement().split(" ");
          SalesID.set(tokens[0]);
          Quantity.set(tokens[1]);
          PairOfStrings secondPair = iterator.next();
          Price.set(secondPair.getRightElement());
          context.write(new Text(), new Text(SalesID.toString()+","+
          (Price * Quantity).toString()));
       } else {
         context.write(new Text("undefined"), new Text("undefined"));
       }
    }
}