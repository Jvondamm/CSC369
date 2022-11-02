import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;
import java.util.Iterator;

public class Reducer3
        extends Reducer<PairOfStrings, PairOfStrings, NullWritable, Text> {
    Text Date = new Text();
    DoubleWritable Price = new DoubleWritable();
    Text City = new Text();
    Text Name = new Text();
    @Override
    public void reduce(PairOfStrings key,
                   Iterable<PairOfStrings> values, Context context)
       throws java.io.IOException, InterruptedException {
      Iterator<PairOfStrings> iterator = values.iterator();
      PairOfStrings firstPair = iterator.next();
      if (firstPair.getLeftElement().toString().equals("ST")) {
         String[] tokens = firstPair.getRightElement().toString().split(" ");
         Name.set(tokens[0].trim());
         City.set(tokens[1].trim());
         while(iterator.hasNext()) {
            PairOfStrings secondPair = iterator.next();
            String[] tokens2 = secondPair.getRightElement().toString().split(" ");
            Date.set(tokens[1].trim());
            Price.set(Double.parseDouble(tokens[1]));
            context.write(NullWritable.get(),
               new Text(Date.toString()+","+
               Name.toString()+","+
               City.toString()+","+
               Double.toString(Price.get())));
         }
      } else {
         context.write(NullWritable.get(), new Text("undefined"));
      }
    }
}