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
    double sum = 0;
    @Override
    public void reduce(PairOfStrings key,
                   Iterable<PairOfStrings> values, Context context)
       throws java.io.IOException, InterruptedException {
      Iterator<PairOfStrings> iterator = values.iterator();
      PairOfStrings firstPair = iterator.next();
      if (firstPair.getLeftElement().toString().equals("S")) {
         Date.set(firstPair.getRightElement().toString().split(" ")[0]);
         StoreID.set(firstPair.getRightElement().toString().split(" ")[1]);
         while(iterator.hasNext()) {
            PairOfStrings secondPair = iterator.next();
            sum += Double.parseDouble(secondPair.getRightElement().toString());
         }
         context.write(NullWritable.get(),
               new Text(StoreID.toString()+","+
               Date.toString()+","+
               Double.toString(sum)));
      } else {
         context.write(NullWritable.get(), new Text("undefined"));
      }
    }
}