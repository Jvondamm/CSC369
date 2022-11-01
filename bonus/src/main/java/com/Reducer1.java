import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;
import java.util.Iterator;

public class Reducer1
        extends Reducer<PairOfStrings, PairOfStrings, NullWritable, Text> {
    DoubleWritable Price = new DoubleWritable();
    IntWritable Quantity = new IntWritable();
    IntWritable SalesID = new IntWritable();
    Text Name = new Text();
    @Override
    public void reduce(PairOfStrings key,
                   Iterable<PairOfStrings> values, Context context)
       throws java.io.IOException, InterruptedException {
      Iterator<PairOfStrings> iterator = values.iterator();
      PairOfStrings firstPair = iterator.next();
      if (firstPair.getLeftElement().toString().equals("P")) {
         Price.set(Double.parseDouble(firstPair.getRightElement().toString()));
         while(iterator.hasNext()) {
            PairOfStrings secondPair = iterator.next();
            String[] tokens = firstPair.getRightElement().toString().split(" ");
            SalesID.set(Integer.parseInt(tokens[0]));
            Quantity.set(Integer.parseInt(tokens[1]));
            context.write(NullWritable.get(), new Text(SalesID.toString()+","+
            Double.toString(Price.get() * Quantity.get())));
         }
      } else {
         context.write(NullWritable.get(), new Text("undefined"));
      }
    }
}