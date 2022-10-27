import java.io.IOException;
import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.*;
import org.apache.log4j.Logger;
import java.util.Iterator;

public class Reducer2
        extends Reducer<PairOfStrings, PairOfStrings, NullWritable, Text> {
    IntWritable Price = new IntWritable();
    Text Date = new Text();
    Text StoreID = new Text();
    Text Name = new Text();
    int sum = 0;
    @Override
    public void reduce(PairOfStrings key,
                   Iterable<PairOfStrings> values, Context context)
       throws java.io.IOException, InterruptedException {
      Iterator<PairOfStrings> iterator = values.iterator();
      PairOfStrings firstPair = iterator.next();
      if (firstPair.getLeftElement().toString().equals("S")) {
         StoreID.set(firstPair.getLeftElement().toString());
         Date.set(firstPair.getRightElement().toString());
         while(iterator.hasNext()) {
            PairOfStrings secondPair = iterator.next();
            sum += Integer.parseInt(secondPair.getLeftElement().get());
         }
         context.write(NullWritable.get(), 
               new Text(Store.toString()+","+
               Date+
               Integer.toString(sum)));
      } else {
         context.write(NullWritable.get(), new Text("undefined"));
      }
    }
}