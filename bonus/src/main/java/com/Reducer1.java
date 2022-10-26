import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;

public class Reducer1
        extends Reducer<PairOfStrings, PairOfStrings, NullWritable, Text> {
    IntWritable Price = new IntWritable();
    IntWritable Quantity = new IntWritable();
    IntWritable SalesID = new IntWritable();
    Text Name = new Text();
    @Override
    public void reduce(PairOfStrings key,
                   Iterable<PairOfStrings> values, Context context)
       throws java.io.IOException, InterruptedException {
      Iterable<PairOfStrings> iterator = values.iterator();
       PairOfStrings firstPair = iterator.next();
       if (firstPair.getLeftElement().toString().equals("L")) {
          String[] tokens = firstPair.getRightElement().split(" ");
          SalesID.set(Integer.parseInt(tokens[0]));
          Quantity.set(Integer.parseInt(tokens[1]));
          PairOfStrings secondPair = iterator.next();
          Price.set(secondPair.getRightElement());
          context.write(NullWritable.get(), new Text(SalesID.toString()+","+
          (Price.get() * Quantity.get()).toString()));
       } else {
         context.write(NullWritable.get(), new Text("undefined"));
       }
    }
}